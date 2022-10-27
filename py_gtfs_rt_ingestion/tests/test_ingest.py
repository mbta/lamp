# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import logging
import os
import pytest

from botocore.stub import ANY

from lib import ConfigType
from lib.error import ArgumentException
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
        ConfigType.VEHICLE_COUNT: NoImplConverter,
        ConfigType.LIGHT_RAIL: NoImplConverter,
        ConfigType.ERROR: NoImplConverter,
    }
    for config_type in ConfigType:
        converter = get_converter(config_type)
        assert isinstance(converter, config_type_map[config_type])


def test_bad_file_names() -> None:
    """
    test that bad filenames are handled appropriately
    """
    # all bad filenames
    files = ["test1", "test2", "test3", "test4"]

    all_error_files = []
    for converter in ingest_files(files):
        assert converter.archive_files == []
        all_error_files += converter.error_files
    assert all_error_files == files
