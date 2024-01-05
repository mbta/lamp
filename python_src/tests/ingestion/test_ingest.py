# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import os
from queue import Queue
import pytest

from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.error import NoImplException
from lamp_py.ingestion.ingest import get_converter
from lamp_py.ingestion.convert_gtfs import GtfsConverter
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter


TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


def test_each_config_type() -> None:
    """
    Test that each config type maps to a converter instance and that they map
    correctly.
    """
    config_type_map = {
        ConfigType.RT_ALERTS: GtfsRtConverter,
        ConfigType.RT_TRIP_UPDATES: GtfsRtConverter,
        ConfigType.RT_VEHICLE_POSITIONS: GtfsRtConverter,
        ConfigType.BUS_TRIP_UPDATES: GtfsRtConverter,
        ConfigType.BUS_VEHICLE_POSITIONS: GtfsRtConverter,
        ConfigType.SCHEDULE: GtfsConverter,
    }
    for config_type, converter_type in config_type_map.items():
        converter = get_converter(config_type, Queue())
        assert isinstance(converter, converter_type)

    bad_config_types = [
        ConfigType.VEHICLE_COUNT,
        ConfigType.LIGHT_RAIL,
        ConfigType.ERROR,
    ]

    for config_type in bad_config_types:
        with pytest.raises(NoImplException):
            converter = get_converter(config_type, Queue())
