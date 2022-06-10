from abc import ABC
from abc import abstractmethod

from enum import auto
from enum import Enum

import pyarrow

from .error import ConfigTypeFromFilenameException


class ConfigType(Enum):
    """
    ConfigType is an Enumuration that is inclusive of all
    configuration types to be processed by this library

    """

    RT_ALERTS = auto()
    RT_TRIP_UPDATES = auto()
    RT_VEHICLE_POSITIONS = auto()
    BUS_TRIP_UPDATES = auto()
    BUS_VEHICLE_POSITIONS = auto()
    VEHICLE_COUNT = auto()

    def __str__(self):
        return self.name

    @classmethod
    def from_filename(cls, filename: str):
        if "mbta.com_realtime_Alerts_enhanced" in filename:
            return cls.RT_ALERTS
        if "mbta.com_realtime_TripUpdates_enhanced" in filename:
            return cls.RT_TRIP_UPDATES
        if "mbta.com_realtime_VehiclePositions_enhanced" in filename:
            return cls.RT_VEHICLE_POSITIONS
        if "com_prod_TripUpdates_enhanced" in filename:
            return cls.BUS_TRIP_UPDATES
        if "com_prod_VehiclePositions_enhanced" in filename:
            return cls.BUS_VEHICLE_POSITIONS
        if "net_vehicleCount" in filename:
            return cls.VEHICLE_COUNT

        raise ConfigTypeFromFilenameException(filename)


class ConfigDetail(ABC):
    """
    Abstract Base Class for all ConfigDetail implementations.

    ConfigDetail classes must implement all methods and properties that are
    defined.
    """

    @property
    @abstractmethod
    def config_type(self) -> ConfigType:
        ...

    @property
    @abstractmethod
    def export_schema(self) -> pyarrow.schema:
        ...

    @abstractmethod
    def record_from_entity(self, entity: dict) -> dict:
        ...
