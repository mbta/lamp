import pytest

from lamp_py.ingestion import ConfigType
from lamp_py.ingestion.error import ConfigTypeFromFilenameException

UPDATE_FILENAME = "2022-01-01T00:00:02Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"

VEHICLE_POSITIONS_FILENAME = "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz"

ALERTS_FILENAME = (
    "2022-01-01T00:00:38Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz"
)


def test_filname_parsing() -> None:
    """
    Check that we are able to get the correct Configuration type for multiple
    filenames
    """
    trip_updates_type = ConfigType.from_filename(UPDATE_FILENAME)
    assert trip_updates_type == ConfigType.RT_TRIP_UPDATES

    vehicle_positions_type = ConfigType.from_filename(
        VEHICLE_POSITIONS_FILENAME
    )
    assert vehicle_positions_type == ConfigType.RT_VEHICLE_POSITIONS

    alerts_type = ConfigType.from_filename(ALERTS_FILENAME)
    assert alerts_type == ConfigType.RT_ALERTS

    with pytest.raises(ConfigTypeFromFilenameException):
        ConfigType.from_filename("this.is.a.bad.filename.json.gz")
