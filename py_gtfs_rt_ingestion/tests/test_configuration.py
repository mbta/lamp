import pytest

from py_gtfs_rt_ingestion import Configuration

def test_filname_parsing():
    """
    Check that we are able to get the correct Configuration type for multiple
    filenames
    """
    trip_updates_type = Configuration.from_filename(
        '2022-01-01T00:00:02Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz')
    assert trip_updates_type == Configuration.RT_TRIP_UPDATES

    vehicle_positions_type = Configuration.from_filename(
        '2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz')
    assert vehicle_positions_type == Configuration.RT_VEHICLE_POSITIONS

    alerts_type = Configuration.from_filename(
        '2022-01-01T00:00:38Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz')
    assert alerts_type == Configuration.RT_ALERTS

    with pytest.raises(Exception):
        Configuration.from_filename('this.is.a.bad.filename.json.gz')

def test_get_schema():
    config = Configuration.RT_VEHICLE_POSITIONS
    schema = config.get_schema()

    with pytest.raises(Exception):
        unimpled_config = Configuration.RT_ALERTS
        unimpled_schema = unimpled_config.get_schema()

