import os
import json

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple, Dict
from dataclasses import dataclass, field
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread
import logging

import pyarrow
from pyarrow import fs

from .error import NoImplException
from .converter import Converter, ConfigType
from .gtfs_rt_detail import GTFSRTDetail
from .logging_utils import ProcessLogger
from .s3_utils import move_s3_objects, write_parquet_file
from .utils import DEFAULT_S3_PREFIX

from .config_rt_alerts import RtAlertsDetail
from .config_rt_bus_vehicle import RtBusVehicleDetail
from .config_rt_bus_trip import RtBusTripDetail
from .config_rt_trip import RtTripDetail
from .config_rt_vehicle import RtVehicleDetail


@dataclass
class TableData:
    tables: List[pyarrow.table] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    next_hour_count: int = 0


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

    def __init__(self, config_type: ConfigType, metadata_queue: Queue) -> None:
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

        self.converted_tables: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

    def convert(self) -> None:
        max_tables_to_convert = 12
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
        # update the active fs to use the s3 filesystem for all loading if the
        # first file starts with s3
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
        max_workers = 6
        yield_threshold = max_workers * 2
        count_past_start_of_hour = 0

        process_logger = ProcessLogger(
            "create_parquet_table",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(
            max_workers=max_workers, initializer=self.thread_init
        ) as pool:
            for (result_dt, result_filename, result_table) in pool.map(
                self.gz_to_pyarrow, self.files
            ):
                # handle error in gz_to_pyarrow processing
                if result_dt is None:
                    self.error_files.append(result_filename)
                    logging.error(
                        f"gz_to_pyarrow exception for: {result_filename}"
                    )
                    continue

                # create key for self.converted_tables dictionary
                timestamp_hour = result_dt.replace(
                    minute=0, second=0, microsecond=0
                )

                # create new self.converted_tables entry for key if it doesn't exist
                if timestamp_hour not in self.converted_tables:
                    self.converted_tables[timestamp_hour] = TableData()

                # loop through all keys in self.converted_tables
                for table_timestamp_hour in list(self.converted_tables.keys()):
                    # add result to matching timestamp_hour key
                    if table_timestamp_hour == timestamp_hour:
                        self.converted_tables[
                            table_timestamp_hour
                        ].files.append(result_filename)
                        self.converted_tables[
                            table_timestamp_hour
                        ].tables.append(result_table)
                    # increment next_hour_count if key is before timestamp_hour
                    elif timestamp_hour > table_timestamp_hour:
                        self.converted_tables[
                            table_timestamp_hour
                        ].next_hour_count += 1

                    # Check if key is ready to yield
                    if (
                        self.converted_tables[
                            table_timestamp_hour
                        ].next_hour_count
                        > yield_threshold
                    ):
                        self.archive_files += self.converted_tables[
                            table_timestamp_hour
                        ].files
                        table = pyarrow.concat_tables(
                            self.converted_tables[table_timestamp_hour].tables
                        )
                        process_logger.add_metadata(
                            file_count=len(
                                self.converted_tables[
                                    table_timestamp_hour
                                ].files
                            ),
                            number_of_rows=table.num_rows,
                        )
                        process_logger.log_complete()
                        yield table
                        del self.converted_tables[table_timestamp_hour]

                        process_logger.add_metadata(
                            file_count=0, number_of_rows=0
                        )
                        process_logger.log_start()

                # check if ready to tend work because processing files past start_of_hour
                # waiting for count_past_start_of_hour > max_workers * 2 should allow for
                # any work from previous hour to finish before exiting
                if result_dt >= self.start_of_hour:
                    count_past_start_of_hour += 1
                    if count_past_start_of_hour > yield_threshold:
                        break
                else:
                    count_past_start_of_hour = 0

        # yeild any remaining tables with next_hour_count > 0
        # guaranteeing that the end of the hour was hit
        # not sure if we would ever actually hit this
        for table_timestamp_hour in list(self.converted_tables.keys()):
            if self.converted_tables[table_timestamp_hour].next_hour_count > 0:
                self.archive_files += self.converted_tables[
                    table_timestamp_hour
                ].files
                table = pyarrow.concat_tables(
                    self.converted_tables[table_timestamp_hour].tables
                )
                process_logger.add_metadata(
                    file_count=len(
                        self.converted_tables[table_timestamp_hour].files
                    ),
                    number_of_rows=table.num_rows,
                )
                process_logger.log_complete()
                yield table
                del self.converted_tables[table_timestamp_hour]

                process_logger.add_metadata(file_count=0, number_of_rows=0)
                process_logger.log_start()

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()

    def record_from_entity(self, entity: dict) -> dict:
        """
        Convert an entity in the ingested json dict into a record for a parquet
        table.
        """

        def drill_entity(drill_keys: str) -> Optional[dict]:
            """Util function for recursively getting data out of entity"""
            ret_dict = entity
            for key in drill_keys.split(",")[1:]:
                value = ret_dict.get(key)
                if value is None:
                    return value
                ret_dict = value
            return ret_dict

        record: dict = {}
        for drill_keys, fields in self.detail.transformation_schema.items():
            pull_dict = drill_entity(drill_keys)
            for get_field in fields:
                if pull_dict is None:
                    record[get_field[-1]] = None
                else:
                    record[get_field[-1]] = pull_dict.get(get_field[0])

        return record

    def gz_to_pyarrow(
        self, filename: str
    ) -> Tuple[Optional[datetime], str, Optional[pyarrow.table]]:
        """
        Accepts filename as string. Converts gzipped json -> pyarrow table.
        Will handle Local or S3 filenames.
        """
        try:
            file_system = current_thread().__dict__["file_system"]
            filename = filename.replace("s3://", "")

            with file_system.open_input_stream(filename) as file:
                json_data = json.load(file)

            # Create empty 'table' as dict of lists for export schema
            table = self.detail.empty_table()

            # parse timestamp info out of the header
            feed_timestamp = json_data["header"]["timestamp"]
            timestamp = datetime.fromtimestamp(feed_timestamp, timezone.utc)

            # for each entity in the list, create a record, add it to the table
            for entity in json_data["entity"]:
                record = self.record_from_entity(entity=entity)
                record.update(
                    {
                        "year": timestamp.year,
                        "month": timestamp.month,
                        "day": timestamp.day,
                        "hour": timestamp.hour,
                        "feed_timestamp": feed_timestamp,
                    }
                )

                for key, value in record.items():
                    table[key].append(value)

        except Exception as _:
            self.thread_init()
            return (None, filename, None)

        return (
            timestamp,
            filename,
            pyarrow.table(table, schema=self.detail.export_schema),
        )

    def write_table(self, table: pyarrow.table) -> None:
        """write the table to our s3 bucket"""
        # don't write if table has no data
        if table.num_rows == 0:
            return

        try:
            s3_prefix = str(self.config_type)
            write_parquet_file(
                table=table,
                config_type=s3_prefix,
                s3_path=os.path.join(
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
