import pytest

from lib import ConfigType
from lib import get_converter
from lib.convert_gtfs import GtfsConverter
from lib.convert_gtfs_rt import GtfsRtConverter
from lib.error import NoImplException


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
    for config_type in ConfigType:
        if config_type == ConfigType.VEHICLE_COUNT:
            continue
        if config_type == ConfigType.LIGHT_RAIL:
            continue
        converter = get_converter(config_type)
        assert isinstance(converter, config_type_map[config_type])


def test_vehicle_count_throws() -> None:
    """
    Test that the vehicle count config type throws a no impl error.

    Remove this test and adjust `test_each_config_type`when its added.
    """
    with pytest.raises(NoImplException):
        # pylint: disable-msg=W0612
        cnvrtr = get_converter(ConfigType.VEHICLE_COUNT)
