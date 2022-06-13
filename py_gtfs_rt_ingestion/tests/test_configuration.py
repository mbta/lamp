import pytest

from datetime import datetime
from py_gtfs_rt_ingestion import Configuration
from py_gtfs_rt_ingestion import ConfigType
from py_gtfs_rt_ingestion.error import ConfigTypeFromFilenameException
from py_gtfs_rt_ingestion.error import NoImplException

update_filename = "2022-01-01T00:00:02Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"

vehicle_positions_filename = "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz"

alerts_filename = (
    "2022-01-01T00:00:38Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz"
)


def test_filname_parsing() -> None:
    """
    Check that we are able to get the correct Configuration type for multiple
    filenames
    """
    trip_updates_type = ConfigType.from_filename(update_filename)
    assert trip_updates_type == ConfigType.RT_TRIP_UPDATES

    vehicle_positions_type = ConfigType.from_filename(
        vehicle_positions_filename
    )
    assert vehicle_positions_type == ConfigType.RT_VEHICLE_POSITIONS

    alerts_type = ConfigType.from_filename(alerts_filename)
    assert alerts_type == ConfigType.RT_ALERTS

    with pytest.raises(ConfigTypeFromFilenameException):
        ConfigType.from_filename("this.is.a.bad.filename.json.gz")


def test_get_schema() -> None:
    config = Configuration(filename=vehicle_positions_filename)
    schema = config.export_schema


def test_create_record() -> None:
    config = Configuration(filename=vehicle_positions_filename)

    entity = {
        "id": "y1628",
        "vehicle": {
            "current_status": "IN_TRANSIT_TO",
            "current_stop_sequence": 11,
            "occupancy_status": "FEW_SEATS_AVAILABLE",
            "position": {
                "bearing": 192,
                "latitude": 42.27057097,
                "longitude": -71.120609509,
            },
            "stop_id": "16498",
            "timestamp": 1640995191,
            "trip": {
                "direction_id": 0,
                "route_id": "32",
                "schedule_relationship": "SCHEDULED",
                "start_date": "20211231",
                "start_time": "18:50:00",
                "trip_id": "50419562",
            },
            "vehicle": {"id": "y1628", "label": "1628"},
        },
    }

    # for now just test that we can do the conversion. we can do validation
    # later on.
    record = config.record_from_entity(entity=entity)
