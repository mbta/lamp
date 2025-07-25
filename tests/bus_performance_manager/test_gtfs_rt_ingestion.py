import os
from unittest import mock
from datetime import datetime, timedelta, date, timezone
from typing import Tuple, List

import polars as pl

from lamp_py.aws.s3 import dt_from_obj_path
from lamp_py.bus_performance_manager.events_gtfs_rt import (
    read_vehicle_positions,
    positions_to_events,
    generate_gtfs_rt_events,
)

from ..test_resources import rt_vehicle_positions as s3_vp


def get_service_date_and_files() -> Tuple[date, List[str]]:
    """
    get a list of service dates and vehicle position files from the vehicle
    positions test files directory. assert that they are all for the same
    service date and return them along with it.

    NOTE: for now, we have to use just 2024 dates, as there are 2023 files that
    only have rail performance test data.
    """
    vp_files = []
    vp_dir = s3_vp.s3_uri
    assert os.path.exists(vp_dir)

    for root, _, files in os.walk(vp_dir):
        for file in files:
            path = os.path.join(root, file)
            if "year=2024" in path:
                vp_files.append(path)

    # get the service date for all the files in the springboard
    service_date = None
    for vp_file in vp_files:
        new_service_date = dt_from_obj_path(vp_file).date()

        if service_date is None:
            service_date = new_service_date

        assert service_date == new_service_date

    assert service_date is not None

    return service_date, vp_files


# schema for vehicle position dataframes
VP_SCHEMA = {
    "route_id": pl.String,
    "direction_id": pl.Int8,
    "trip_id": pl.String,
    "stop_id": pl.String,
    "stop_sequence": pl.Int64,
    "start_time": pl.String,
    "service_date": pl.String,
    "vehicle_id": pl.String,
    "vehicle_label": pl.String,
    "current_status": pl.String,
    "vehicle_timestamp": pl.Datetime,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
}

# schema for vehicle events dataframes
VE_SCHEMA = {
    "service_date": pl.String,
    "route_id": pl.String,
    "trip_id": pl.String,
    "start_time": pl.Int64,
    "start_dt": pl.Datetime,
    "stop_count": pl.UInt32,
    "direction_id": pl.Int8,
    "stop_id": pl.String,
    "stop_sequence": pl.Int64,
    "vehicle_id": pl.String,
    "vehicle_label": pl.String,
    "current_status": pl.String,
    "gtfs_travel_to_dt": pl.Datetime,
    "gtfs_arrival_dt": pl.Datetime,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
}


@mock.patch("lamp_py.utils.gtfs_utils.object_exists")
def test_gtfs_rt_to_bus_events(exists_patch: mock.MagicMock) -> None:
    """
    generate vehicle event dataframes from gtfs realtime vehicle position files.
    inspect them to ensure they
        * are not empty
        * have records matching known properties

    generate an empty vechicle event frame and ensure it has the correct columns.
    """
    exists_patch.return_value = True

    service_date, vp_files = get_service_date_and_files()

    # get the bus vehicle events for these files / service date
    bus_vehicle_events = generate_gtfs_rt_events(service_date=service_date, gtfs_rt_files=vp_files)

    assert not bus_vehicle_events.is_empty()

    for col, data_type in bus_vehicle_events.schema.items():
        assert VE_SCHEMA[col] == data_type

    # the following properties are known to be in the consumed dataset
    y1808_events = bus_vehicle_events.filter((pl.col("vehicle_id") == "y1808") & (pl.col("trip_id") == "61348621"))
    assert not y1808_events.is_empty()

    for event in y1808_events.to_dicts():
        assert event["route_id"] == "504"
        assert event["direction_id"] == 0

        if event["stop_id"] == "173":
            assert event["gtfs_travel_to_dt"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=1, second=19, tzinfo=timezone.utc
            )
            assert event["gtfs_arrival_dt"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=2, second=34, tzinfo=timezone.utc
            )

        if event["stop_id"] == "655":
            assert event["gtfs_travel_to_dt"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=50, second=31, tzinfo=timezone.utc
            )
            assert event["gtfs_arrival_dt"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=53, second=29, tzinfo=timezone.utc
            )

        if event["stop_id"] == "903":
            assert event["gtfs_travel_to_dt"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=3, second=39, tzinfo=timezone.utc
            )
            assert event["gtfs_arrival_dt"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=11, second=3, tzinfo=timezone.utc
            )

    # the following properties are known to be in the consumed dataset
    y1329_events = bus_vehicle_events.filter((pl.col("vehicle_id") == "y1329") & (pl.col("trip_id") == "61884885-OL1"))

    assert not y1329_events.is_empty()

    for event in y1329_events.to_dicts():
        assert event["route_id"] == "741"

        # there is null data in the incoming data set for these two
        assert event["direction_id"] == 0
        assert event["stop_id"] is not None

        # no arrival time at this stop
        if event["stop_id"] == "12005":
            assert event["gtfs_travel_to_dt"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=47, second=23, tzinfo=timezone.utc
            )
            assert event["gtfs_arrival_dt"] is None

        if event["stop_id"] == "17091":
            assert event["gtfs_travel_to_dt"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=52, second=41, tzinfo=timezone.utc
            )
            assert event["gtfs_arrival_dt"] is None

    # get an empty dataframe by reading the same files but for events the day prior.
    previous_service_date = service_date - timedelta(days=1)
    empty_bus_vehicle_events = generate_gtfs_rt_events(service_date=previous_service_date, gtfs_rt_files=vp_files)

    assert empty_bus_vehicle_events.is_empty()

    for col, data_type in empty_bus_vehicle_events.schema.items():
        assert VE_SCHEMA[col] == data_type

    pl.concat([bus_vehicle_events, empty_bus_vehicle_events])


@mock.patch("lamp_py.utils.gtfs_utils.object_exists")
def test_read_vehicle_positions(exists_patch: mock.MagicMock) -> None:
    """
    test that vehicle positions can be read from files and return a df with the
    correct schema.
    """
    exists_patch.return_value = True
    service_date, vp_files = get_service_date_and_files()

    vehicle_positions = read_vehicle_positions(service_date=service_date, gtfs_rt_files=vp_files)

    for col, data_type in vehicle_positions.schema.items():
        assert VP_SCHEMA[col] == data_type


def route_one() -> pl.DataFrame:
    """
    straight forward vehicle positions dataframe
    """
    data = {
        "route_id": ["1", "1", "1", "1"],
        "direction_id": [0, 0, 0, 0],
        "trip_id": ["101", "101", "101", "101"],
        "stop_id": ["1", "1", "2", "2"],
        "stop_sequence": [1, 1, 2, 2],
        "start_time": ["08:45:00", "08:45:00", "08:45:00", "08:45:00"],
        "service_date": ["20240601", "20240601", "20240601", "20240601"],
        "vehicle_id": ["y1001", "y1001", "y1001", "y1001"],
        "vehicle_label": ["1001", "1001", "1001", "1001"],
        "current_status": [
            "IN_TRANSIT_TO",
            "STOPPED_AT",
            "IN_TRANSIT_TO",
            "STOPPED_AT",
        ],
        "vehicle_timestamp": [
            datetime(year=2024, month=6, day=1, hour=8, minute=45),
            datetime(year=2024, month=6, day=1, hour=8, minute=47),
            datetime(year=2024, month=6, day=1, hour=8, minute=55),
            datetime(year=2024, month=6, day=1, hour=8, minute=57),
        ],
        "latitude": [42.3516, 42.3516, 42.3516, 42.3516],
        "longitude": [-71.0668, -71.0668, -71.0668, -71.0668],
    }

    return pl.DataFrame(data, schema=VP_SCHEMA)


def route_two() -> pl.DataFrame:
    """
    multiple in transit to and stopped positions per stop
    """
    data = {
        "route_id": ["2", "2", "2", "2", "2", "2"],
        "direction_id": [0, 0, 0, 0, 0, 0],
        "trip_id": ["202", "202", "202", "202", "202", "202"],
        "stop_id": ["1", "1", "1", "2", "2", "2"],
        "stop_sequence": [1, 1, 1, 2, 2, 2],
        "start_time": [
            "09:45:00",
            "09:45:00",
            "09:45:00",
            "09:45:00",
            "09:45:00",
            "09:45:00",
        ],
        "service_date": [
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
        ],
        "vehicle_id": ["y2002", "y2002", "y2002", "y2002", "y2002", "y2002"],
        "vehicle_label": ["2002", "2002", "2002", "2002", "2002", "2002"],
        "current_status": [
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
            "STOPPED_AT",
            "IN_TRANSIT_TO",
            "STOPPED_AT",
            "STOPPED_AT",
        ],
        "vehicle_timestamp": [
            datetime(year=2024, month=6, day=1, hour=9, minute=45),
            datetime(year=2024, month=6, day=1, hour=9, minute=46),
            datetime(year=2024, month=6, day=1, hour=9, minute=47),
            datetime(year=2024, month=6, day=1, hour=9, minute=55),
            datetime(year=2024, month=6, day=1, hour=9, minute=57),
            datetime(year=2024, month=6, day=1, hour=9, minute=59),
        ],
        "latitude": [
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
        ],
        "longitude": [
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
        ],
    }

    return pl.DataFrame(data, schema=VP_SCHEMA)


def route_three() -> pl.DataFrame:
    """
    No In Transit To data, only stopped at
    """
    data = {
        "route_id": ["3", "3", "3", "3", "3", "3"],
        "direction_id": [0, 0, 0, 0, 0, 0],
        "trip_id": ["303", "303", "303", "303", "303", "303"],
        "stop_id": ["1", "1", "1", "2", "2", "2"],
        "stop_sequence": [1, 1, 1, 2, 2, 2],
        "start_time": [
            "10:45:00",
            "10:45:00",
            "10:45:00",
            "10:45:00",
            "10:45:00",
            "10:45:00",
        ],
        "service_date": [
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
        ],
        "vehicle_id": ["y3003", "y3003", "y3003", "y3003", "y3003", "y3003"],
        "vehicle_label": ["3003", "3003", "3003", "3003", "3003", "3003"],
        "current_status": [
            "STOPPED_AT",
            "STOPPED_AT",
            "STOPPED_AT",
            "STOPPED_AT",
            "STOPPED_AT",
            "STOPPED_AT",
        ],
        "vehicle_timestamp": [
            datetime(year=2024, month=6, day=1, hour=10, minute=45),
            datetime(year=2024, month=6, day=1, hour=10, minute=46),
            datetime(year=2024, month=6, day=1, hour=10, minute=47),
            datetime(year=2024, month=6, day=1, hour=10, minute=55),
            datetime(year=2024, month=6, day=1, hour=10, minute=57),
            datetime(year=2024, month=6, day=1, hour=10, minute=59),
        ],
        "latitude": [
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
        ],
        "longitude": [
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
        ],
    }

    return pl.DataFrame(data, schema=VP_SCHEMA)


def route_four() -> pl.DataFrame:
    """
    No Sopped At data, only In Transit To
    """
    data = {
        "route_id": ["4", "4", "4", "4", "4", "4"],
        "direction_id": [0, 0, 0, 0, 0, 0],
        "trip_id": ["404", "404", "404", "404", "404", "404"],
        "stop_id": ["1", "1", "1", "2", "2", "2"],
        "stop_sequence": [1, 1, 1, 2, 2, 2],
        "start_time": [
            "11:45:00",
            "11:45:00",
            "11:45:00",
            "11:45:00",
            "11:45:00",
            "11:45:00",
        ],
        "service_date": [
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
            "20240601",
        ],
        "vehicle_id": ["y4004", "y4004", "y4004", "y4004", "y4004", "y4004"],
        "vehicle_label": ["4004", "4004", "4004", "4004", "4004", "4004"],
        "current_status": [
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
            "IN_TRANSIT_TO",
        ],
        "vehicle_timestamp": [
            datetime(year=2024, month=6, day=1, hour=11, minute=45),
            datetime(year=2024, month=6, day=1, hour=11, minute=46),
            datetime(year=2024, month=6, day=1, hour=11, minute=47),
            datetime(year=2024, month=6, day=1, hour=11, minute=55),
            datetime(year=2024, month=6, day=1, hour=11, minute=57),
            datetime(year=2024, month=6, day=1, hour=11, minute=59),
        ],
        "latitude": [
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
            42.3516,
        ],
        "longitude": [
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
            -71.0668,
        ],
    }

    return pl.DataFrame(data, schema=VP_SCHEMA)


def test_positions_to_events() -> None:
    """
    test that vehicle positions can be converted to vehicle events
    correctly for different edge cases.
    """
    route_one_positions = route_one()
    route_one_events = positions_to_events(vehicle_positions=route_one_positions)
    assert len(route_one_events) == 2

    route_two_positions = route_two()
    route_two_events = positions_to_events(vehicle_positions=route_two_positions)
    assert len(route_two_events) == 2

    route_three_positions = route_three()
    route_three_events = positions_to_events(vehicle_positions=route_three_positions)
    assert len(route_three_events) == 2

    route_four_positions = route_four()
    route_four_events = positions_to_events(vehicle_positions=route_four_positions)
    assert len(route_four_events) == 2

    empty_positions = pl.DataFrame(schema=VP_SCHEMA)
    empty_events = positions_to_events(vehicle_positions=empty_positions)
    assert len(empty_events) == 0

    # confirm that vehicle events have the same ordered schema and can be
    # concatted.
    pl.concat(
        [
            route_one_events,
            route_two_events,
            route_three_events,
            route_four_events,
            empty_events,
        ]
    )
