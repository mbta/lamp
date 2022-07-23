import json
import logging
import os

from datetime import datetime, timezone
from typing import Optional, cast
from concurrent.futures import ThreadPoolExecutor

import pyarrow
from pyarrow import fs

from .error import NoImplException
from .converter import Converter, ConfigType
from .gtfs_rt_detail import GTFSRTDetail

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

    def convert(self, files: list[str]) -> list[tuple[str, pyarrow.Table]]:
        pa_table = pyarrow.table(
            self.detail.empty_table(), schema=self.detail.export_schema
        )

        # this is the default pool size for a ThreadPoolExecutor as of py3.8
        cpu_count = cast(
            int, os.cpu_count() if os.cpu_count() is not None else 1
        )
        pool_size = min(32, cpu_count + 4)

        logging.info(
            "Creating pool with %d threads, %d cores available",
            pool_size,
            os.cpu_count(),
        )

        with ThreadPoolExecutor(max_workers=pool_size) as executor:
            for result in executor.map(self.gz_to_pyarrow, files):
                if result is not None:
                    pa_table = pyarrow.concat_tables([pa_table, result])

        logging.info(
            "Completed converting %d files with config %s",
            len(files),
            self.config_type,
        )

        # return a list with a single prefix, table tuple
        return [(str(self.config_type), pa_table)]

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

    def gz_to_pyarrow(self, filename: str) -> Optional[str]:
        """
        Accepts filename as string. Converts gzipped json -> pyarrow table.

        Will handle Local or S3 filenames.
        """
        logging.info("Converting %s to Parquet Table", filename)
        try:
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

            # skip files that have been generated after the start of the current
            # hour. don't add them to archive or error so that they are picked
            # up next go around.
            if timestamp >= self.start_of_hour:
                logging.debug("skipping %s", filename)
                return None

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

            self.archive_files.append(filename)
            return pyarrow.table(table, schema=self.detail.export_schema)

        except Exception as exception:
            logging.error("Error converting %s", filename)
            logging.exception(exception)
            self.error_files.append(filename)
            return None

    @property
    def partition_cols(self) -> list[str]:
        return ["year", "month", "day", "hour"]
