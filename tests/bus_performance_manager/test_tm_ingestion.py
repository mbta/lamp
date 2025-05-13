import os
from datetime import datetime

from _pytest.monkeypatch import MonkeyPatch
import polars as pl

from lamp_py.bus_performance_manager.events_tm import generate_tm_events

from ..test_resources import (
    tm_geo_node_file,
    tm_route_file,
    tm_trip_file,
    tm_vehicle_file,
    tm_stop_crossings,
)


def test_tm_to_bus_events(monkeypatch: MonkeyPatch) -> None:
    """
    run tests on each file in the test files tm stop crossings directory
    """
    monkeypatch.setattr(
        "lamp_py.bus_performance_manager.events_tm.tm_geo_node_file",
        tm_geo_node_file,
    )
    monkeypatch.setattr(
        "lamp_py.bus_performance_manager.events_tm.tm_route_file",
        tm_route_file,
    )
    monkeypatch.setattr(
        "lamp_py.bus_performance_manager.events_tm.tm_trip_file",
        tm_trip_file,
    )
    monkeypatch.setattr(
        "lamp_py.bus_performance_manager.events_tm.tm_vehicle_file",
        tm_vehicle_file,
    )

    tm_sc_dir = tm_stop_crossings.s3_uri
    print(tm_sc_dir)
    assert os.path.exists(tm_sc_dir)

    for filename in os.listdir(tm_sc_dir):
        check_stop_crossings(os.path.join(tm_sc_dir, filename))


def check_stop_crossings(stop_crossings_filepath: str) -> None:
    """
    run checks on the dataframes produced by running generate_tm_events on
    transit master stop crossing files.
    """
    print(f"processing: {stop_crossings_filepath}")
    # Remove the .parquet extension and get the date
    filename = os.path.basename(stop_crossings_filepath)
    date_str = filename.replace(".parquet", "")[1:]
    service_date_est = datetime.strptime(date_str, "%Y%m%d")
    service_date_est = pl.datetime(
        year=service_date_est.year, month=service_date_est.month, day=service_date_est.day
    ).dt.replace_time_zone("America/New_York")

    service_date_utc = service_date_est.dt.convert_time_zone("UTC")
    # this is the df of all useful records from the stop crossings files
    raw_stop_crossings = (
        pl.scan_parquet(stop_crossings_filepath)
        .filter(pl.col("ACT_ARRIVAL_TIME").is_not_null() | pl.col("ACT_DEPARTURE_TIME").is_not_null())
        .collect()
    )

    # run the generate tm events function on our input files
    bus_events = generate_tm_events(tm_files=[stop_crossings_filepath])

    # ensure data has been extracted from the filepath
    assert not bus_events.is_empty()

    # ensure we didn't lose any Revenue data from the raw dataset when joining
    assert len(bus_events) == len(raw_stop_crossings.filter((pl.col("IsRevenue") == "R")))

    # check that crossings without trips are garage pullouts
    bus_garages = {
        "soham",
        "lynn",
        "prwb",
        "charl",
        "cabot",
        "arbor",
        "qubus",
        "somvl",
    }
    non_trip_events = bus_events.filter(pl.col("trip_id").is_null())
    assert set(non_trip_events["stop_id"]).issubset(bus_garages)

    # check that all scheduled, arrival and departure timestamps happen after the start of the service date
    assert bus_events.filter(
        (pl.col("tm_actual_arrival_dt") < service_date_utc)
        | (pl.col("tm_actual_departure_dt") < service_date_utc)
        | (pl.col("tm_scheduled_time_dt") < service_date_utc)
    ).is_empty()
    # check that all departure times are after the arrival times
    assert bus_events.filter(pl.col("tm_actual_arrival_dt") > pl.col("tm_actual_departure_dt")).is_empty()

    # check that there are no leading zeros on route ids
    assert bus_events.filter(
        pl.col("route_id").str.starts_with("0")
        | pl.col("trip_id").str.starts_with("0")
        | pl.col("stop_id").str.starts_with("0")
    ).is_empty()

    # scheduled_time_sam, actual_arrival_time_sam, actual_departure_time_sam - these are unprocessed straight from TM.
    # sam = seconds after midnight
    # intended for comparison/data validation

    # e.g.
    # tm_actual_departure_dt 2024-06-01 13:13:47 UTC - seconds after midnight = 47627
    # actual_departure_time - 33227
    # 47627-33227 = 14400 / 60 / 60 = 4 hrs - (UTC -> EDT (UTC-04:00))
    assert not bus_events["tm_scheduled_time_sam"].has_nulls()
    assert not bus_events["tm_actual_arrival_time_sam"].has_nulls()
    assert not bus_events["tm_actual_departure_time_sam"].has_nulls()
    assert (
        bus_events.select(pl.col("tm_actual_departure_time_sam").ge(pl.col("tm_actual_arrival_time_sam")))
        .filter(False)
        .is_empty()
    )

    # check that scheduled/departure/arrival dt are equal to the seconds after midnight representation
    # this also checks that all the Datetimes are in the same timezone - but the conversion and test for EST is out of scope
    assert (
        (bus_events.select(pl.col("tm_actual_departure_dt") - service_date_utc)).to_series().dt.total_seconds()
        - bus_events["tm_actual_departure_time_sam"]
        == 0
    ).all()
    assert (
        (bus_events.select(pl.col("tm_actual_arrival_dt") - service_date_utc)).to_series().dt.total_seconds()
        - bus_events["tm_actual_arrival_time_sam"]
        == 0
    ).all()
    assert (
        (bus_events.select(pl.col("tm_scheduled_time_dt") - service_date_utc)).to_series().dt.total_seconds()
        - bus_events["tm_scheduled_time_sam"]
        == 0
    ).all()
