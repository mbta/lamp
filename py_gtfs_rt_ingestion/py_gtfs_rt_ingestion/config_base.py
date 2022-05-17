from abc import ABC
from abc import abstractmethod

from enum import auto
from enum import Enum

import pyarrow

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

class ConfigDetail(ABC):
    """
    Abstract Base Class for all ConfigDetail implementations.

    ConfigDetail classes must implement all methods and properties that are defined.
    """
    @property
    @abstractmethod
    def config_type(self) -> ConfigType: ...

    @property
    @abstractmethod
    def export_schema(self) -> pyarrow.schema: ...

    @abstractmethod
    def record_from_entity(self, entity: dict) -> dict: ...