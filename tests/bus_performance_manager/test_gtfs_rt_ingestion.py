import os
from datetime import datetime, timedelta

import polars as pl

from lamp_py.aws.s3 import get_datetime_from_partition_path
from lamp_py.bus_performance_manager.gtfs_rt_ingestion import (
    generate_gtfs_rt_events,
)

from ..test_resources import LocalFileLocaions


def test_gtfs_rt_to_bus_events() -> None:
    """
    generate vehicle event dataframes from gtfs realtime vehicle position files.
    inspect them to ensure they
        * are not empty
        * have records matching known properties

    generate an empty vechicle event frame and ensure it has the correct columns.
    """
    vp_files = []
    vp_dir = LocalFileLocaions.vehicle_positions.get_s3_path()
    assert os.path.exists(vp_dir)

    for root, _, files in os.walk(vp_dir):
        for file in files:
            path = os.path.join(root, file)
            if "year=2024" in path:
                vp_files.append(path)

    # get the service date for all the files in the springboard
    service_date = None
    for vp_file in vp_files:
        new_service_date = get_datetime_from_partition_path(vp_file).date()

        if service_date is None:
            service_date = new_service_date

        assert service_date == new_service_date

    assert service_date is not None

    # get the bus vehicle events for these files / service date
    bus_vehicle_events = generate_gtfs_rt_events(
        service_date=service_date, gtfs_rt_files=vp_files
    )

    assert not bus_vehicle_events.is_empty()

    # the following properties are known to be in the consumed dataset
    y1808_events = bus_vehicle_events.filter(
        (pl.col("vehicle_id") == "y1808") & (pl.col("trip_id") == "61348621")
    )

    assert not y1808_events.is_empty()

    for event in y1808_events.to_dicts():
        assert event["route_id"] == "504"
        assert event["direction_id"] == 0

        if event["stop_id"] == "173":
            assert event["travel_towards_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=1, second=19
            )
            assert event["arrival_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=2, second=34
            )

        if event["stop_id"] == "655":
            assert event["travel_towards_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=50, second=31
            )
            assert event["arrival_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=53, second=29
            )

        if event["stop_id"] == "903":
            assert event["travel_towards_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=3, second=39
            )
            assert event["arrival_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=13, minute=11, second=3
            )

    # the following properties are known to be in the consumed dataset
    y1329_events = bus_vehicle_events.filter(
        (pl.col("vehicle_id") == "y1329")
        & (pl.col("trip_id") == "61884885-OL1")
    )

    assert not y1329_events.is_empty()

    for event in y1329_events.to_dicts():
        assert event["route_id"] == "741"

        # there is null data in the incoming data set for these two
        assert event["direction_id"] == 0
        assert event["stop_id"] is not None

        # no arrival time at this stop
        if event["stop_id"] == "12005":
            assert event["travel_towards_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=47, second=23
            )
            assert event["arrival_gtfs"] is None

        if event["stop_id"] == "17091":
            assert event["travel_towards_gtfs"] == datetime(
                year=2024, month=6, day=1, hour=12, minute=52, second=41
            )
            assert event["arrival_gtfs"] is None

    # get an empty dataframe by reading the same files but for events the day prior.
    previous_service_date = service_date - timedelta(days=1)
    empty_bus_vehicle_events = generate_gtfs_rt_events(
        service_date=previous_service_date, gtfs_rt_files=vp_files
    )

    assert empty_bus_vehicle_events.is_empty()

    assert set(bus_vehicle_events.columns) == set(
        empty_bus_vehicle_events.columns
    )
