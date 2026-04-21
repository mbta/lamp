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

    # this filetype is currently being added from delta into our incoming
    # bucket. we haven't looked into it yet, and its ingestion remains
    # unimplimented.
    LIGHT_RAIL = auto()

    ERROR = auto()

    def __str__(self) -> str:
        return self.name

    @classmethod
    def filename_patterns(cls) -> dict[ConfigType, list[str]]:
        """
        Return mapping of ConfigType to their filename patterns.

        Multiple patterns can match the same ConfigType.
        """
        return {
            cls.RT_ALERTS: ["mbta.com_realtime_Alerts_enhanced"],
            cls.RT_TRIP_UPDATES: [
                "mbta.com_realtime_TripUpdates_enhanced",
                "concentrate_TripUpdates_enhanced.json",
            ],
            cls.RT_VEHICLE_POSITIONS: [
                "mbta.com_realtime_VehiclePositions_enhanced",
                "concentrate_VehiclePositions_enhanced.json",
            ],
            cls.BUS_TRIP_UPDATES: ["com_prod_TripUpdates_enhanced"],
            cls.BUS_VEHICLE_POSITIONS: ["com_prod_VehiclePositions_enhanced"],
            cls.VEHICLE_COUNT: ["net_vehicleCount"],
            cls.SCHEDULE: ["MBTA_GTFS.zip"],
            cls.LIGHT_RAIL: ["LightRailRawGPS"],
            cls.DEV_GREEN_RT_TRIP_UPDATES: ["https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_TripUpdates_enhanced"],
            cls.DEV_GREEN_RT_VEHICLE_POSITIONS: [
                "https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json"
            ],
        }

    @classmethod
    def from_filename(cls, filename: str) -> ConfigType:
        """
        Figure out which config type to use for a given filename.

        Raise a ConfigTypeFromFilenameException if unable to determine.
        """
        for config_type, patterns in cls.filename_patterns().items():
            for pattern in patterns:
                if pattern in filename:
                    return config_type

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
