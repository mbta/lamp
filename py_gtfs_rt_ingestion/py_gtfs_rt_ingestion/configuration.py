from abc import ABC
from abc import abstractmethod

from enum import Enum
from enum import auto
from pathlib import Path
import pyarrow

from config_rt_vehicle_pos import RtVehiclePositionDetails

class ConfigType(Enum):
    RT_ALERTS = auto()
    RT_TRIP_UPDATES = auto()
    RT_VEHICLE_POSITIONS = auto()
    BUS_TRIP_UPDATES = auto()
    BUS_VEHICLE_POSITIONS = auto()
    VEHICLE_COUNT = auto()

"""
All configuation details classes must be configured 
in same format as ConfigDetails.
"""
class ConfigDetails(ABC):
    @property
    @abstractmethod
    def config_type(self) -> ConfigType: ...

    @property
    @abstractmethod
    def export_schema(self) -> pyarrow.schema: ...

    @abstractmethod
    def record_from_entity(self, entity: dict) -> dict: ...

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
            self.details = RtVehiclePositionDetails()
        # if 'mbta.com_realtime_TripUpdates_enhanced' in filename:
        #     self.import_type = ImportType.RT_TRIP_UPDATES
        # if 'mbta.com_realtime_VehiclePositions_enhanced' in filename:
        #     self.import_type = ImportType.RT_VEHICLE_POSITIONS
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



