import json
import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
    timezone,
)
from queue import Queue
from threading import current_thread
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

import polars as pl
import pyarrow
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as pd

from lamp_py.aws.s3 import (
    move_s3_objects,
    file_list_from_s3,
    download_file,
    upload_file,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.config_rt_alerts import RtAlertsDetail
from lamp_py.ingestion.config_busloc_trip import RtBusTripDetail
from lamp_py.ingestion.config_busloc_vehicle import RtBusVehicleDetail
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.config_rt_vehicle import RtVehicleDetail
from lamp_py.ingestion.converter import ConfigType, Converter
from lamp_py.ingestion.error import NoImplException
from lamp_py.ingestion.error import IgnoreIngestion
from lamp_py.ingestion.gtfs_rt_detail import GTFSRTDetail
from lamp_py.ingestion.utils import (
    GTFS_RT_HASH_COL,
    hash_gtfs_rt_table,
    hash_gtfs_rt_parquet,
)
from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_SPRINGBOARD,
    S3_ERROR,
    S3_ARCHIVE,
)


@dataclass
class TableData:
    """
    Data structure for holding data related to yielding a parquet table

    tables: list of pyarrow tables that will joined together for final table yield
    files: list of files that make up tables
    """

    table: Optional[pyarrow.Table] = None
    files: List[str] = field(default_factory=list)


class GtfsRtConverter(Converter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    https_mbta_integration.mybluemix.net_vehicleCount.gz
    """

    def __init__(self, config_type: ConfigType, metadata_queue: Queue[Optional[str]]) -> None:
        Converter.__init__(self, config_type, metadata_queue)

        # Depending on filename, assign self.details to correct implementation
        # of GTFSRTDetail class.
        self.detail: GTFSRTDetail

        if config_type == ConfigType.RT_ALERTS:
            self.detail = RtAlertsDetail()
        elif config_type == ConfigType.RT_TRIP_UPDATES:
            self.detail = RtTripDetail()
        elif config_type == ConfigType.RT_VEHICLE_POSITIONS:
            self.detail = RtVehicleDetail()
        elif config_type == ConfigType.BUS_VEHICLE_POSITIONS:
            self.detail = RtBusVehicleDetail()
        elif config_type == ConfigType.BUS_TRIP_UPDATES:
            self.detail = RtBusTripDetail()
        elif config_type == ConfigType.DEV_GREEN_RT_TRIP_UPDATES:
            self.detail = RtTripDetail()
        elif config_type == ConfigType.DEV_GREEN_RT_VEHICLE_POSITIONS:
            self.detail = RtVehicleDetail()
        elif config_type == ConfigType.LIGHT_RAIL:
            raise IgnoreIngestion("Ignore LIGHT_RAIL files")
        else:
            raise NoImplException(f"No Specialization for {config_type}")

        self.tmp_folder = "/tmp/gtfs-rt-continuous"

        self.data_parts: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

    def convert(self) -> None:
        max_tables_to_convert = 2
        process_logger = ProcessLogger(
            "parquet_table_creator",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        try:
            for table in self.process_files():
                if table.num_rows == 0:
                    continue

                self.continuous_pq_update(table)
                pool = pyarrow.default_memory_pool()
                pool.release_unused()
                table_count += 1
                process_logger.add_metadata(table_count=table_count)
                # limit number of tables produced on each event loop
                if table_count >= max_tables_to_convert:
                    break

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.data_parts = {}
            self.move_s3_files()
            self.clean_local_folders()

    def thread_init(self) -> None:
        """
        initialize the filesystem in each convert thread

        update the active fs to use the s3 filesystem for all loading if the
        first file starts with s3
        """
        thread_data = current_thread()
        if self.files and self.files[0].startswith("s3://"):
            thread_data.__dict__["file_system"] = fs.S3FileSystem()
        else:
            thread_data.__dict__["file_system"] = fs.LocalFileSystem()

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        iterate through all of the files to be converted

        only yield a new table when table size crosses over min_rows of yield_check
        """
        max_workers = 4

        process_logger = ProcessLogger(
            "create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(max_workers=max_workers, initializer=self.thread_init) as pool:
            for result_dt, result_filename, rt_data in pool.map(self.gz_to_pyarrow, self.files):
                # errors in gtfs_rt conversions are handled in the gz_to_pyarrow
                # function. if one is encountered, the datetime will be none. log
                # the error and move on to the next file.
                if result_dt is None:
                    self.error_files.append(result_filename)
                    logging.error(
                        "gz_to_pyarrow exception when loading: %s",
                        result_filename,
                    )
                    continue

                # create key for self.data_parts dictionary
                dt_part = datetime(
                    year=result_dt.year,
                    month=result_dt.month,
                    day=result_dt.day,
                )
                file_log = ProcessLogger("gz_to_pyarrow", table_mbs=round(rt_data.nbytes/(1024*1024),2))
                file_log.log_start()
                # create new self.table_groups entry for key if it doesn't exist
                if dt_part not in self.data_parts:
                    self.data_parts[dt_part] = TableData()
                    self.data_parts[dt_part].table = self.detail.transform_for_write(rt_data)
                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [
                            self.data_parts[dt_part].table,
                            self.detail.transform_for_write(rt_data),
                        ]
                    )
                

                self.data_parts[dt_part].files.append(result_filename)
                file_log.log_complete()
                yield from self.yield_check(process_logger)

        # yield any remaining tables
        yield from self.yield_check(process_logger, min_bytes=-1)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()

    def yield_check(self, process_logger: ProcessLogger, min_bytes: int = 1000*1024*1024) -> Iterable[pyarrow.table]:
        """
        yield all tables in the data_parts map that have been sufficiently
        processed.

        @min_rows - how many rows the table must have to be yielded
        @process_logger - a process logger for the conversion process. log a
            completion and reset before a file is yielded.

        @yield pyarrow.table - a concatenated table of gtfs realtime data.
        """
        for iter_ts in list(self.data_parts.keys()):
            table = self.data_parts[iter_ts].table
            if table is not None and table.nbytes > min_bytes:
                self.archive_files += self.data_parts[iter_ts].files

                process_logger.add_metadata(
                    file_count=len(self.data_parts[iter_ts].files),
                    number_of_rows=table.num_rows,
                    table_mbs=round(table.nbytes/(1024*1024), 2)
                )
                process_logger.log_complete()
                # reset process logger
                process_logger.add_metadata(file_count=0, number_of_rows=0, print_log=False)
                process_logger.log_start()

                yield table
                self.data_parts[iter_ts].table = None
                del self.data_parts[iter_ts]

    def gz_to_pyarrow(self, filename: str) -> Tuple[Optional[datetime], str, Optional[pyarrow.table]]:
        """
        Convert a gzipped json of gtfs realtime data into a pyarrow table. This
        function is executed inside of a thread, so all exceptions must be
        handled internally.

        @filename file of gtfs rt data to be converted (file system chosen by
            GtfsRtConverter in thread_init)

        @return Optional[datetime] - datetime contained in header of gtfs rt
            feed. (returns None if an Exception is thrown during conversion)
        @return str - input filename with s3 prefix stripped out.
        @return Optional[pyarrow.table] - the pyarrow table of gtfs rt data that
            has been converted. (returns None if an Exception is thrown during
            conversion)
        """
        try:
            file_system = current_thread().__dict__["file_system"]
            filename = filename.replace("s3://", "")

            # some of our older files are named incorrectly, with a simple
            # .json suffix rather than a .json.gz suffix. in those cases, the
            # s3 open_input_stream is unable to deduce the correct compression
            # algo and fails with a UnicodeDecodeError. catch this failure and
            # retry using a gzip compression algo. (EAFP Style)
            try:
                with file_system.open_input_stream(filename) as file:
                    json_data = json.load(file)
            except UnicodeDecodeError as _:
                with file_system.open_input_stream(filename, compression="gzip") as file:
                    json_data = json.load(file)

            # parse timestamp info out of the header
            feed_timestamp = json_data["header"]["timestamp"]
            timestamp = datetime.fromtimestamp(feed_timestamp, timezone.utc)

            table = pyarrow.Table.from_pylist(json_data["entity"], schema=self.detail.import_schema)

            table = table.append_column(
                "year",
                pyarrow.array([timestamp.year] * table.num_rows, pyarrow.uint16()),
            )
            table = table.append_column(
                "month",
                pyarrow.array([timestamp.month] * table.num_rows, pyarrow.uint8()),
            )
            table = table.append_column(
                "day",
                pyarrow.array([timestamp.day] * table.num_rows, pyarrow.uint8()),
            )
            table = table.append_column(
                "feed_timestamp",
                pyarrow.array([feed_timestamp] * table.num_rows, pyarrow.uint64()),
            )

        except FileNotFoundError as _:
            return (None, filename, None)
        except Exception as _:
            self.thread_init()
            return (None, filename, None)

        return (
            timestamp,
            filename,
            table,
        )

    def partition_dt(self, table: pyarrow.Table) -> datetime:
        """
        verify partition structure of pyarrow Table

        :param table: pyarrow Table to verify

        :return: datetime of table partition
        """
        partitions = {
            "year": 0,
            "month": 0,
            "day": 0,
        }
        for col in partitions:
            unique_list = pc.unique(table.column(col)).to_pylist()

            assert (
                len(unique_list) == 1
            ), f"{self.config_type} Table column {col} had {len(unique_list)} unique elements"
            partitions[col] = unique_list[0]

        return datetime(
            year=partitions["year"],
            month=partitions["month"],
            day=partitions["day"],
        )

    def sync_with_s3(self, local_path: str) -> bool:
        """
        Sync local_path with S3 object

        :param local_path: local tmp path file to sync

        :return bool: True if local_path is available, else False
        """
        if os.path.exists(local_path):
            return True

        local_folder = local_path.replace(os.path.basename(local_path), "")
        os.makedirs(local_folder, exist_ok=True)

        s3_files = file_list_from_s3(
            S3_SPRINGBOARD,
            file_prefix=local_path.replace(f"{self.tmp_folder}/", ""),
        )
        if len(s3_files) == 1:
            s3_path = s3_files[0].replace("s3://", "")
            download_file(s3_path, local_path)
            return True

        return False

    def make_hash_dataset(self, table: pyarrow.Table, local_path: str) -> None:
        """
        create dataset, with hash column, that will be written to parquet file

        :param table: pyarrow Table
        :param local_path: path to local parquet file
        """
        log = ProcessLogger("make_hash_datset")
        log.log_start()
        table = hash_gtfs_rt_table(table)
        self.out_ds = pd.dataset(table)

        if self.sync_with_s3(local_path):
            hash_gtfs_rt_parquet(local_path)
            # RT_ALERTS parquet files contain columns with nested structure types
            # if a new nested field is ingested, combining of the new and existing nested column is not possible
            # this try/except is meant to catch that error and reset the schema for the sevice day to the new nested structure
            # RT_ALERTS updates are essentially the same throughout a service day so resetting the
            # dataset will have minimal impact on archived data
            try:
                self.out_ds = pd.dataset(
                    [
                        pd.dataset(table),
                        pd.dataset(local_path),
                    ]
                )
            except pyarrow.ArrowTypeError as exception:
                if self.config_type == ConfigType.RT_ALERTS:
                    self.out_ds = pd.dataset(table)
                else:
                    raise exception
        log.log_complete()

    # pylint: disable=R0914
    # pylint too many local variables (more than 15)
    def write_local_pq(self, table: pyarrow.Table, local_path: str) -> None:
        """
        merge pyarrow Table with existing local_path parquet file

        :param table: pyarrow Table
        :param local_path: path to local parquet file
        """
        logger = ProcessLogger("write_local_pq", local_path=local_path, table_rows=table.num_rows)
        logger.log_start()

        self.make_hash_dataset(table, local_path)

        unique_ts_min = pc.min(table.column("feed_timestamp")).as_py() - (60 * 45)

        no_hash_schema = self.out_ds.schema.remove(self.out_ds.schema.get_field_index(GTFS_RT_HASH_COL))

        with tempfile.TemporaryDirectory() as temp_dir:
            hash_pq_path = os.path.join(temp_dir, "hash.parquet")
            upload_path = os.path.join(temp_dir, "upload.parquet")
            hash_writer = pq.ParquetWriter(hash_pq_path, schema=self.out_ds.schema, compression="zstd", compression_level=3)
            upload_writer = pq.ParquetWriter(upload_path, schema=no_hash_schema, compression="zstd", compression_level=3)

            partitions = pc.unique(
                self.out_ds.to_table(columns=[self.detail.partition_column]).column(self.detail.partition_column)
            )
            for part in partitions:
                logger.add_metadata(table_part=str(part), part_rows=0, part_mbs=0)
                write_table = (
                    pl.DataFrame(
                        self.out_ds.to_table(
                            filter=(
                                (pc.field(self.detail.partition_column) == part)
                                & (pc.field("feed_timestamp") >= unique_ts_min)
                            )
                        )
                    )
                    .sort(by=["feed_timestamp"])
                    .unique(subset=GTFS_RT_HASH_COL, keep="first")
                    .to_arrow()
                    .cast(self.out_ds.schema)
                )
                if write_table.num_rows > 0:
                    hash_writer.write_table(write_table)
                    upload_writer.write_table(write_table.drop_columns(GTFS_RT_HASH_COL))
                    logger.add_metadata(part_rows=write_table.num_rows, part_mbs=round(write_table.nbytes/(1024*1024),2))

                write_table = self.out_ds.to_table(
                    filter=(
                        (pc.field(self.detail.partition_column) == part) & (pc.field("feed_timestamp") < unique_ts_min)
                    )
                )
                if write_table.num_rows > 0:
                    hash_writer.write_table(write_table)
                    upload_writer.write_table(write_table.drop_columns(GTFS_RT_HASH_COL))
                    logger.add_metadata(part_rows=write_table.num_rows, part_mbs=round(write_table.nbytes/(1024*1024),2))

            hash_writer.close()
            upload_writer.close()

            os.replace(hash_pq_path, local_path)
            upload_file(
                upload_path,
                local_path.replace(self.tmp_folder, S3_SPRINGBOARD),
            )

        logger.log_complete()

    # pylint: enable=R0914

    def continuous_pq_update(self, table: pyarrow.Table) -> None:
        """
        Continuous updating of local parquet files that are synced with S3
        """
        log = ProcessLogger("continuous_pq_update")
        log.log_start()
        try:
            partition_dt = self.partition_dt(table)

            local_path = os.path.join(
                self.tmp_folder,
                LAMP,
                str(self.config_type),
                f"year={partition_dt.year}",
                f"month={partition_dt.month}",
                f"day={partition_dt.day}",
                f"{partition_dt.isoformat()}.parquet",
            )

            table = table.drop_columns(["year", "month", "day"])

            log.add_metadata(local_path=local_path)

            self.write_local_pq(table, local_path)
            self.send_metadata(local_path.replace(self.tmp_folder, S3_SPRINGBOARD))

            log.log_complete()

        except Exception as exception:
            shutil.rmtree(
                os.path.join(
                    self.tmp_folder,
                    LAMP,
                    str(self.config_type),
                ),
                ignore_errors=True,
            )
            self.error_files += self.archive_files
            self.archive_files = []
            log.log_failure(exception)

    def clean_local_folders(self) -> None:
        """
        clean local temp folders
        """
        days_to_keep = 2
        root_folder = os.path.join(
            self.tmp_folder,
            LAMP,
            str(self.config_type),
        )
        paths = {}
        for w_dir, _, files in os.walk(root_folder):
            if len(files) == 0:
                continue
            paths[datetime.strptime(w_dir, f"{root_folder}/year=%Y/month=%m/day=%d")] = w_dir

        # remove all local day folders except two most recent
        for key in sorted(paths.keys())[:-days_to_keep]:
            shutil.rmtree(paths[key])

    def move_s3_files(self) -> None:
        """
        move archive and error files to their respective s3 buckets.
        """
        if len(self.error_files) > 0:
            self.error_files = move_s3_objects(
                self.error_files,
                os.path.join(S3_ERROR, LAMP),
            )

        if len(self.archive_files) > 0:
            self.archive_files = move_s3_objects(
                self.archive_files,
                os.path.join(S3_ARCHIVE, LAMP),
            )
