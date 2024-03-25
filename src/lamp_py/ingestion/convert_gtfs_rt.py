import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Queue
from threading import current_thread
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow
from pyarrow import fs

from lamp_py.aws.s3 import move_s3_objects, write_parquet_file
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .config_rt_alerts import RtAlertsDetail
from .config_busloc_trip import RtBusTripDetail
from .config_busloc_vehicle import RtBusVehicleDetail
from .config_rt_trip import RtTripDetail
from .config_rt_vehicle import RtVehicleDetail
from .converter import ConfigType, Converter
from .error import NoImplException
from .gtfs_rt_detail import GTFSRTDetail
from .utils import DEFAULT_S3_PREFIX


@dataclass
class TableData:
    """
    Data structure for holding data related to yielding a parquet table

    tables: list of pyarrow tables that will joined together for final table yield
    files: list of files that make up tables
    next_hr_cnt: keeps track of how many files in the next hour have been
                 processed, when this hits a certain threshold the table
                 can be yielded
    """

    table: Optional[pyarrow.table] = None
    files: List[str] = field(default_factory=list)
    next_hr_cnt: int = 0


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

        # get the start of the current hour. any files written after this will
        # not be ingested until the next go around.
        now = datetime.now(tz=timezone.utc)
        self.start_of_hour = now.replace(minute=0, second=0, microsecond=0)

        self.table_groups: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

    def convert(self) -> None:
        max_tables_to_convert = 6
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
                self.write_table(table)
                self.move_s3_files()

                # only count table if it contains data
                if table.num_rows > 0:
                    table_count += 1

                process_logger.add_metadata(table_count=table_count)

                # limit number of tables produced on each event loop
                if table_count >= max_tables_to_convert:
                    break

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()

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

        # this is the number of files created in an hour after the processing
        # hour that will trigger a table to be yielding for writing
        yield_threshold = max(10, max_workers * 3)

        process_logger = ProcessLogger(
            "create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(
            max_workers=max_workers, initializer=self.thread_init
        ) as pool:
            for result_dt, result_filename, result_table in pool.map(
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

                # create key for self.table_groups dictionary
                timestamp_hr = result_dt.replace(
                    minute=0, second=0, microsecond=0
                )

                # create new self.table_groups entry for key if it doesn't exist
                if timestamp_hr not in self.table_groups:
                    self.table_groups[timestamp_hr] = TableData()

                # process results into self.table_groups
                for iter_ts, table_group in self.table_groups.items():
                    # add result to matching timestamp_hr key
                    if iter_ts == timestamp_hr:
                        table_group.files.append(result_filename)
                        if table_group.table is None:
                            table_group.table = self.detail.transform_for_write(
                                result_table
                            )
                        else:
                            table_group.table = pyarrow.concat_tables(
                                [
                                    table_group.table,
                                    self.detail.transform_for_write(
                                        result_table
                                    ),
                                ]
                            )
                        table_group.next_hr_cnt = 0
                    # increment next_hr_cnt if key is before timestamp_hr
                    elif timestamp_hr > iter_ts:
                        table_group.next_hr_cnt += 1

                yield from self.yield_check(yield_threshold, process_logger)

                # check if ready to end work because processing files past start_of_hour
                # waiting for count of files > yield_threshold should allow for
                # any work from previous hour to finish before exiting
                if (
                    result_dt >= self.start_of_hour
                    and len(self.table_groups[timestamp_hr].files)
                    > yield_threshold
                ):
                    break

        # yield any remaining tables with next_hr_cnt > 0
        # guaranteeing that the end of the hour was hit
        # not sure if we would ever actually hit this
        yield from self.yield_check(0, process_logger)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()

    def yield_check(
        self, yield_threshold: int, process_logger: ProcessLogger
    ) -> Iterable[pyarrow.table]:
        """
        yield all tables in the table_groups map that have been sufficiently
        processed.

        gtfs realtime files are processed chronologically in a thread pool and
        table groups are collections of gtfs realtime tables that are from the
        same hour. if they were being processed in series we could yield a
        table as soon as a realtime file from the next hour was processed.
        instead, ensure that a sufficient number for files from the next hour
        have been processed.

        additionally, do not yield any tables from the current hour, as we want
        limit the number of parquet files generated (ideally one per hour) and
        more data for the current hour will be coming in later.

        @yield_threshold - how many files from the next hour have to be
            processed before considering _this_ hour complete.
        @process_logger - a process logger for the conversion process. log a
            completion and reset before a file is yielded.

        @yield pyarrow.table - a concatenated table of all the gtfs realtime
            data over the corse of an hour.
        """
        for iter_ts in list(self.table_groups.keys()):
            if (
                self.table_groups[iter_ts].next_hr_cnt > yield_threshold
                and iter_ts < self.start_of_hour
            ):
                self.archive_files += self.table_groups[iter_ts].files

                table = self.table_groups[iter_ts].table

                assert table is not None

                process_logger.add_metadata(
                    file_count=len(self.table_groups[iter_ts].files),
                    number_of_rows=table.num_rows,
                )
                process_logger.log_complete()

                process_logger.add_metadata(file_count=0, number_of_rows=0)
                process_logger.log_start()

                yield table
                del self.table_groups[iter_ts]

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

    def write_table(self, table: pyarrow.table) -> None:
        """write the table to our s3 bucket"""
        # don't write if table has no data
        if table.num_rows == 0:
            return

        try:
            s3_prefix = str(self.config_type)

            sort_log = ProcessLogger(
                "pyarrow_sort_by", table_rows=table.num_rows
            )
            sort_log.log_start()
            if self.detail.table_sort_order is not None:
                table = table.sort_by(self.detail.table_sort_order)
            sort_log.log_complete()

            write_parquet_file(
                table=table,
                file_type=s3_prefix,
                s3_dir=os.path.join(
                    os.environ["SPRINGBOARD_BUCKET"],
                    DEFAULT_S3_PREFIX,
                    s3_prefix,
                ),
                partition_cols=["year", "month", "day", "hour"],
                visitor_func=self.send_metadata,
            )

        except Exception:
            self.error_files += self.archive_files
            self.archive_files = []

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