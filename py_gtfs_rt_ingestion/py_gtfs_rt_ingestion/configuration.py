from pathlib import Path

import pyarrow

from .config_base import ConfigType

from .config_rt_vehicle import RtVehicleDetails
from .config_rt_alerts import RtAlertsDetails
from .config_rt_trip import RtTripDetails

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
    def __init__(self, filename: str) -> None:
        self.file_path = Path(filename)

        if 'mbta.com_realtime_Alerts_enhanced' in self.file_path.name:
            self.details = RtAlertsDetails()
        elif 'mbta.com_realtime_TripUpdates_enhanced' in filename:
            self.details = RtTripDetails()
        elif 'mbta.com_realtime_VehiclePositions_enhanced' in filename:
            self.details = RtVehicleDetails()
        else:
            raise Exception("Bad Configuration from filename %s" % filename)

    @property
    def config_type(self) -> ConfigType:
        return self.details.config_type

    @property
    def export_schema(self) -> pyarrow.schema:
        return self.details.export_schema

    def record_from_entity(self, entity: dict) -> dict:
        return self.details.record_from_entity(entity)



