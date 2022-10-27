# use annotations to type hint a method with the type of the enclosing class
# https://stackoverflow.com/a/33533514
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import List, Tuple

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
    SCHEDULE = auto()
    LIGHT_RAIL = auto()

    ERROR = auto()

    def __str__(self) -> str:
        return self.name

    @classmethod
    def from_filename(cls, filename: str) -> ConfigType:
        """
        Figure out which config type to use for a given filename. Raise a
        ConfigTypeFromFilenameException if unable to determine.
        """
        # pylint: disable-msg=R0911
        # disable too many returns error message
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
        if "MBTA_GTFS.zip" in filename:
            return cls.SCHEDULE
        if "LightRailRawGPS" in filename:
            return cls.LIGHT_RAIL

        raise ConfigTypeFromFilenameException(filename)

    def is_gtfs(self) -> bool:
        """Is this a GTFS config?"""
        return self in [self.SCHEDULE]

    def is_gtfs_rt(self) -> bool:
        """Is this a GTFS Real Time config?"""
        return self in [
            self.RT_ALERTS,
            self.RT_TRIP_UPDATES,
            self.RT_VEHICLE_POSITIONS,
            self.BUS_TRIP_UPDATES,
            self.BUS_VEHICLE_POSITIONS,
            self.VEHICLE_COUNT,
            self.LIGHT_RAIL,
        ]


class Converter(ABC):
    """
    Abstract Base Class for converters that take incoming files and convert them
    into pyarrow tables.
    """

    def __init__(self, config_type: ConfigType) -> None:
        self.config_type = config_type
        self.archive_files: list[str] = []
        self.error_files: list[str] = []

    @abstractmethod
    def get_tables(self) -> List[Tuple[str, pyarrow.Table]]:
        """
        get a list of tables and their s3 prefixes so they can be written to s3
        """

    @abstractmethod
    def add_file(self, file: str) -> bool:
        """
        add a file to the converter to ingest and format.
        @return is this the last file to ingest
        """

    @abstractmethod
    def reset(self) -> None:
        """
        reset this converter for another round of files to ingest and transform
        into tables
        """

    @property
    @abstractmethod
    def partition_cols(self) -> list[str]:
        """
        list of columns to patition out of parquet tables
        """
