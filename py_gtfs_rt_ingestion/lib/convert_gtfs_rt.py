import json

from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pyarrow
from pyarrow import fs

from .error import NoImplException
from .converter import Converter, ConfigType
from .gtfs_rt_detail import GTFSRTDetail
from .logging_utils import ProcessLogger

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

    def __init__(self, config_type: ConfigType) -> None:
        Converter.__init__(self, config_type)

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

        self.current_time: Optional[datetime] = None
        self.table = pyarrow.table(
            self.detail.empty_table(), schema=self.detail.export_schema
        )
        self.next_table: Optional[pyarrow.table] = None

    def get_tables(self) -> List[Tuple[str, pyarrow.Table]]:
        return [(str(self.config_type), self.table)]

    def reset(self) -> None:
        self.current_time = None
        self.table = self.next_table
        self.next_table = None

        self.archive_files = []
        self.error_files = []

    def add_file(self, file: str) -> bool:
        process_logger = ProcessLogger(
            "convert_single_gtfs_rt",
            config_type=str(self.config_type),
            filename=file,
        )
        process_logger.log_start()

        try:
            timestamp, table = self.gz_to_pyarrow(file)

            # skip files that have been generated after the start of the current
            # hour. don't add them to archive or error so that they are picked
            # up next go around.
            if timestamp >= self.start_of_hour:
                return False

            if self.current_time is None:
                self.current_time = timestamp
            else:
                same_hour = (
                    timestamp.year == self.current_time.year
                    and timestamp.month == self.current_time.month
                    and timestamp.day == self.current_time.day
                    and timestamp.hour == self.current_time.hour
                )

                if not same_hour:
                    self.next_table = table
                    return True

            # check to see if this timestamp matches the current timestamps hours
            self.table = pyarrow.concat_tables([self.table, table])
            self.archive_files.append(file)

            process_logger.log_complete()
            return False

        except Exception as exception:
            process_logger.log_failure(exception)
            self.error_files.append(file)
            return False

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
        if filename.startswith("s3://"):
            active_fs = fs.S3FileSystem()
            file_to_load = str(filename).replace("s3://", "")
        else:
            active_fs = fs.LocalFileSystem()
            file_to_load = filename

        with active_fs.open_input_stream(file_to_load) as file:
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

    @property
    def partition_cols(self) -> list[str]:
        return ["year", "month", "day", "hour"]
