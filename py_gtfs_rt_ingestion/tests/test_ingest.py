# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import os
import pytest

from botocore.stub import ANY

from lib import ConfigType
from lib.error import NoImplException
from lib.ingest import ingest_files, get_converter, NoImplConverter
from lib.s3_utils import file_list_from_s3
from lib.convert_gtfs import GtfsConverter
from lib.convert_gtfs_rt import GtfsRtConverter

from .test_s3_utils import s3_stub

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
        converter = get_converter(config_type)
        assert isinstance(converter, converter_type)

    bad_config_types = [
        ConfigType.VEHICLE_COUNT,
        ConfigType.LIGHT_RAIL,
        ConfigType.ERROR,
    ]

    for config_type in bad_config_types:
        with pytest.raises(NoImplException):
            converter = get_converter(config_type)
