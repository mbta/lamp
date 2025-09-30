# use annotations to type hint a method with the type of the enclosing class
# https://stackoverflow.com/a/33533514
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from queue import Queue
from typing import List, Optional

from enum import auto
from enum import Enum

from lamp_py.runtime_utils.lamp_exception import ConfigTypeFromFilenameException


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
    DEV_GREEN_RT_TRIP_UPDATES = auto()
    DEV_GREEN_RT_VEHICLE_POSITIONS = auto()
    DEV_BLUE_RT_TRIP_UPDATES = auto()
    DEV_BLUE_RT_VEHICLE_POSITIONS = auto()
    # this filetype is currently being added from delta into our incoming
    # bucket. we haven't looked into it yet, and its ingestion remains
    # unimplimented.
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
        # pylint: disable-msg=R0911,R0912
        # disable too many returns error message
        if "mbta.com_realtime_Alerts_enhanced" in filename:
            return cls.RT_ALERTS

        if "mbta.com_realtime_TripUpdates_enhanced" in filename:
            return cls.RT_TRIP_UPDATES
        if "concentrate_TripUpdates_enhanced.json" in filename:
            return cls.RT_TRIP_UPDATES

        if "mbta.com_realtime_VehiclePositions_enhanced" in filename:
            return cls.RT_VEHICLE_POSITIONS
        if "concentrate_VehiclePositions_enhanced.json" in filename:
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

        if "https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_TripUpdates_enhanced" in filename:
            return cls.DEV_GREEN_RT_TRIP_UPDATES
        if "https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json" in filename:
            return cls.DEV_GREEN_RT_VEHICLE_POSITIONS

        if "https_mbta_gtfs_s3_dev_blue.s3.amazonaws.com_rtr_TripUpdates_enhanced" in filename:
            return cls.DEV_GREEN_RT_TRIP_UPDATES
        if "https_mbta_gtfs_s3_dev_blue.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json" in filename:
            return cls.DEV_GREEN_RT_VEHICLE_POSITIONS

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
            self.DEV_GREEN_RT_VEHICLE_POSITIONS,
            self.DEV_GREEN_RT_TRIP_UPDATES,
            self.DEV_BLUE_RT_VEHICLE_POSITIONS,
            self.DEV_BLUE_RT_TRIP_UPDATES,
        ]


class Converter(ABC):
    """
    Abstract Base Class for converters that take incoming files and convert them
    into pyarrow tables.
    """

    def __init__(self, config_type: ConfigType, metadata_queue: Queue[Optional[str]]) -> None:
        self.config_type = config_type
        self.files: List[str] = []
        self.metadata_queue: Queue[Optional[str]] = metadata_queue

    def add_files(self, files: List[str]) -> None:
        """add files to this converter"""
        self.files += files

    def send_metadata(self, written_file: str) -> None:
        """send metadata path to rds writer process"""
        self.metadata_queue.put(written_file)

    @abstractmethod
    def convert(self) -> None:
        """
        convert files to pyarrow tables, write them to s3 as parquete, and move
        files from incoming to archive (or error)
        """
