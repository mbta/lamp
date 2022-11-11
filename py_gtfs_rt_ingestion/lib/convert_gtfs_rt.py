import os
import json

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple
from multiprocessing import Queue

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

        self.error_files: List[str] = []
        self.archive_files: List[str] = []

        self.active_fs = fs.LocalFileSystem()

    def convert(self) -> None:
        process_logger = ProcessLogger(
            "parquet_table_creator",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        for table in self.process_files():
            self.write_table(table)
            self.move_s3_files()

            # only count table if it contains data
            if table.num_row > 0:
                table_count += 1

        process_logger.add_metadata(table_count=table_count)
        process_logger.log_complete()

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        iterate through all of the files to be converted, yielding a new table
        everytime the timestamps cross over an hour.
        """
        table = pyarrow.table(
            self.detail.empty_table(),
            schema=self.detail.export_schema,
        )
        current_time = None

        process_logger = ProcessLogger(
            "create_parquet_table",
            config_type=str(self.config_type),
        )
        file_count = 0

        # update the active fs to use the s3 filesystem for all loading if the
        # first file starts with s3
        if self.files and self.files[0].startswith("s3://"):
            self.active_fs = fs.S3FileSystem()

        for file in self.files:
            try:
                timestamp, new_data = self.gz_to_pyarrow(file)

                # skip files that have been generated after the start of the
                # current hour. don't add them to archive or error so that they
                # are picked up next go around.
                if timestamp >= self.start_of_hour:
                    break

                if current_time is None:
                    current_time = timestamp

                same_hour = (
                    timestamp.year == current_time.year
                    and timestamp.month == current_time.month
                    and timestamp.day == current_time.day
                    and timestamp.hour == current_time.hour
                )

                # if the next table crossed over into the next hour, then write
                # out the current table, move archive and error s3 files, and
                # reset the state of the converter.
                if not same_hour:
                    process_logger.add_metadata(
                        file_count=file_count, number_of_rows=table.num_rows
                    )
                    process_logger.log_complete()
                    yield table

                    self.error_files = []
                    self.archive_files = []
                    table = pyarrow.table(
                        self.detail.empty_table(),
                        schema=self.detail.export_schema,
                    )
                    current_time = timestamp
                    file_count = 0

                    process_logger.add_metadata(file_count=0)
                    process_logger.log_start()

                table = pyarrow.concat_tables([table, new_data])
                self.archive_files.append(file)
                file_count += 1

            except Exception:
                self.error_files.append(file)

        process_logger.add_metadata(
            file_count=file_count, number_of_rows=table.num_rows
        )
        process_logger.log_complete()
        yield table

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

    def gz_to_pyarrow(self, filename: str) -> Tuple[datetime, pyarrow.table]:
        """
        Accepts filename as string. Converts gzipped json -> pyarrow table.
        Will handle Local or S3 filenames.
        """
        filename = filename.replace("s3://", "")
        with self.active_fs.open_input_stream(filename) as file:
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

        return (
            timestamp,
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
            move_s3_objects(
                self.error_files,
                os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
            )

        if len(self.archive_files) > 0:
            move_s3_objects(
                self.archive_files,
                os.path.join(os.environ["ARCHIVE_BUCKET"], DEFAULT_S3_PREFIX),
            )
