from ipaddress import v4_int_to_packed
from pathlib import Path

import pyarrow

from .config_base import ConfigType

from .config_rt_vehicle import RtVehicleDetail
from .config_rt_alerts import RtAlertsDetail
from .config_rt_trip import RtTripDetail

class Configuration:
    """
    Configuration that handles the specifics of each of our JSON record types.

    https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    https_mbta_integration.mybluemix.net_vehicleCount.gz
    """
    def __init__(self,
                 config_type: ConfigType=None,
                 filename: str=None) -> None:
        """
        Depending on filename, assign self.details to correct implementation of 
        ConfigDetail class. 
        """
        if config_type is None:
            config_type = ConfigType.from_filename(filename)

        if config_type == ConfigType.RT_ALERTS:
            self.detail = RtAlertsDetail()
        elif config_type == ConfigType.RT_TRIP_UPDATES:
            self.detail = RtTripDetail()
        elif config_type == ConfigType.RT_VEHICLE_POSITIONS:
            self.detail = RtVehicleDetail()
        else:
            raise Exception("Bad Configuration Type %s" % config_type)

    @property
    def config_type(self) -> ConfigType:
        return self.detail.config_type

    @property
    def export_schema(self) -> pyarrow.schema:
        return self.detail.export_schema

    def record_from_entity(self, entity: dict) -> dict:
        return self.detail.record_from_entity(entity)



