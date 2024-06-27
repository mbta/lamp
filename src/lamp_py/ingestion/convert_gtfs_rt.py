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
from lamp_py.ingestion.gtfs_rt_detail import GTFSRTDetail
from lamp_py.ingestion.utils import (
    DEFAULT_S3_PREFIX,
    GTFS_RT_HASH_COL,
    hash_gtfs_rt_table,
    hash_gtfs_rt_parquet,
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

    def __init__(
        self, config_type: ConfigType, metadata_queue: Queue[Optional[str]]
    ) -> None:
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
        else:
            raise NoImplException(f"No Specialization for {config_type}")

        self.tmp_folder = "/tmp/gtfs-rt-continuous"

        self.data_parts: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

    def convert(self) -> None:
        max_tables_to_convert = 50
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
            self.clean_local_folders()
            self.move_s3_files()

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

        only yield a new table when the timestamps cross over an hour.
        """
        max_workers = 4

        process_logger = ProcessLogger(
            "create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(
            max_workers=max_workers, initializer=self.thread_init
        ) as pool:
            for result_dt, result_filename, rt_data in pool.map(
                self.gz_to_pyarrow, self.files
            ):
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
                dt_part = result_dt.replace(minute=0, second=0, microsecond=0)

                # create new self.table_groups entry for key if it doesn't exist
                if dt_part not in self.data_parts:
                    self.data_parts[dt_part] = TableData()

                self.archive_files.append(result_filename)
                self.data_parts[dt_part].files.append(result_filename)

                if self.data_parts[dt_part].table is None:
                    self.data_parts[dt_part].table = (
                        self.detail.transform_for_write(rt_data)
                    )
                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [
                            self.data_parts[dt_part].table,
                            self.detail.transform_for_write(rt_data),
                        ]
                    )

                yield from self.yield_check(process_logger)

        # yield any remaining tables
        yield from self.yield_check(process_logger, min_rows=0)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()

    def yield_check(
        self, process_logger: ProcessLogger, min_rows: int = 1_000_000
    ) -> Iterable[pyarrow.table]:
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
            if table is not None and table.num_rows > min_rows:
                process_logger.add_metadata(
                    file_count=len(self.data_parts[iter_ts].files),
                    number_of_rows=table.num_rows,
                )
                process_logger.log_complete()

                process_logger.add_metadata(file_count=0, number_of_rows=0)
                process_logger.log_start()

                yield table
                del self.data_parts[iter_ts]

    def gz_to_pyarrow(
        self, filename: str
    ) -> Tuple[Optional[datetime], str, Optional[pyarrow.table]]:
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
                with file_system.open_input_stream(
                    filename, compression="gzip"
                ) as file:
                    json_data = json.load(file)

            # parse timestamp info out of the header
            feed_timestamp = json_data["header"]["timestamp"]
            timestamp = datetime.fromtimestamp(feed_timestamp, timezone.utc)

            table = pyarrow.Table.from_pylist(
                json_data["entity"], schema=self.detail.import_schema
            )

            table = table.append_column(
                "year",
                pyarrow.array(
                    [timestamp.year] * table.num_rows, pyarrow.uint16()
                ),
            )
            table = table.append_column(
                "month",
                pyarrow.array(
                    [timestamp.month] * table.num_rows, pyarrow.uint8()
                ),
            )
            table = table.append_column(
                "day",
                pyarrow.array(
                    [timestamp.day] * table.num_rows, pyarrow.uint8()
                ),
            )
            table = table.append_column(
                "hour",
                pyarrow.array(
                    [timestamp.hour] * table.num_rows, pyarrow.uint8()
                ),
            )
            table = table.append_column(
                "feed_timestamp",
                pyarrow.array(
                    [feed_timestamp] * table.num_rows, pyarrow.uint64()
                ),
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
            "hour": 0,
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
            hour=partitions["hour"],
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
            os.environ["SPRINGBOARD_BUCKET"],
            file_prefix=local_path.replace(f"{self.tmp_folder}/", ""),
        )
        if len(s3_files) == 1:
            s3_path = s3_files[0].replace("s3://", "")
            download_file(s3_path, local_path)
            return True

        return False

    def write_local_pq(self, table: pyarrow.Table, local_path: str) -> None:
        """
        merge pyarrow Table with existing local_path parquet file

        :param table: pyarrow Table
        :param local_path: path to local parquet file
        """
        part_col = self.detail.partition_column
        table = hash_gtfs_rt_table(table)
        out_ds = pd.dataset(table)

        if self.sync_with_s3(local_path) is True:
            hash_gtfs_rt_parquet(local_path)
            out_ds = pd.dataset(
                [
                    pd.dataset(table),
                    pd.dataset(local_path),
                ]
            )

        partitions = pc.unique(
            out_ds.to_table(columns=[part_col]).column(part_col)
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_pq = os.path.join(temp_dir, "temp.parquet")
            with pq.ParquetWriter(tmp_pq, schema=out_ds.schema) as writer:
                for part in partitions:
                    part_filter = pc.field(part_col) == part
                    table = pl.from_arrow(out_ds.to_table(filter=part_filter))
                    writer.write_table(
                        table.sort(
                            by=["feed_timestamp"],
                        )
                        .unique(subset=GTFS_RT_HASH_COL, keep="first")
                        .to_arrow()
                        .cast(out_ds.schema)
                    )

            os.replace(tmp_pq, local_path)

    def upload_local_pq(self, local_path: str) -> None:
        """
        upload local parquet file to S3
        """
        part_col = self.detail.partition_column
        upload_path = local_path.replace(
            self.tmp_folder, os.environ["SPRINGBOARD_BUCKET"]
        )
        ds = pd.dataset(local_path)
        write_columns = ds.schema.names
        write_columns.remove(GTFS_RT_HASH_COL)
        write_schema = ds.schema.remove(
            ds.schema.get_field_index(GTFS_RT_HASH_COL)
        )
        part_cols = pc.unique(ds.to_table(columns=[part_col]).column(part_col))
        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_pq = os.path.join(temp_dir, "temp.parquet")
            with pq.ParquetWriter(tmp_pq, schema=write_schema) as writer:
                for part in part_cols:
                    pq_filter = pc.field(part_col) == part
                    writer.write_table(
                        ds.to_table(columns=write_columns, filter=pq_filter)
                        .cast(write_schema)
                        .sort_by(self.detail.table_sort_order)
                    )
            upload_file(tmp_pq, upload_path)

        self.send_metadata(upload_path)

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
                DEFAULT_S3_PREFIX,
                str(self.config_type),
                f"year={partition_dt.year}",
                f"month={partition_dt.month}",
                f"day={partition_dt.day}",
                f"hour={partition_dt.hour}",
                f"{partition_dt.isoformat()}.parquet",
            )

            table = table.drop_columns(["year", "month", "day", "hour"])

            log.add_metadata(local_path=local_path)

            self.write_local_pq(table, local_path)

            self.upload_local_pq(local_path)

            log.log_complete()

        except Exception as exception:
            shutil.rmtree(
                os.path.join(
                    self.tmp_folder,
                    DEFAULT_S3_PREFIX,
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
        hours_to_keep = 2
        root_folder = os.path.join(
            self.tmp_folder,
            DEFAULT_S3_PREFIX,
            str(self.config_type),
        )
        paths = {}
        for w_dir, _, files in os.walk(root_folder):
            if len(files) == 0:
                continue
            paths[
                datetime.strptime(
                    w_dir, f"{root_folder}/year=%Y/month=%m/day=%d/hour=%H"
                )
            ] = w_dir

        # remove all local hour folders except two most recent
        for key in sorted(paths.keys())[:-hours_to_keep]:
            shutil.rmtree(paths[key])

    def move_s3_files(self) -> None:
        """
        move archive and error files to their respective s3 buckets.
        """
        if len(self.error_files) > 0:
            self.error_files = move_s3_objects(
                self.error_files,
                os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
            )

        if len(self.archive_files) > 0:
            self.archive_files = move_s3_objects(
                self.archive_files,
                os.path.join(os.environ["ARCHIVE_BUCKET"], DEFAULT_S3_PREFIX),
            )
