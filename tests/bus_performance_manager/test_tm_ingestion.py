import os
from datetime import datetime

from _pytest.monkeypatch import MonkeyPatch
import polars as pl

from lamp_py.bus_performance_manager.tm_ingestion import generate_tm_events

from ..test_resources import LocalFileLocaions


def test_tm_to_bus_events(monkeypatch: MonkeyPatch) -> None:
    """
    run tests on each file in the test files tm stop crossings directory
    """
    monkeypatch.setattr(
        "lamp_py.bus_performance_manager.tm_ingestion.RemoteFileLocations",
        LocalFileLocaions,
    )

    tm_sc_dir = LocalFileLocaions.tm_stop_crossing.get_s3_path()
    assert os.path.exists(tm_sc_dir)

    for filename in os.listdir(tm_sc_dir):
        check_stop_crossings(os.path.join(tm_sc_dir, filename))


def check_stop_crossings(stop_crossings_filepath: str) -> None:
    """
    run checks on the dataframes produced by running generate_tm_events on
    transit master stop crossing files.
    """
    # Remove the .parquet extension and get the date
    filename = os.path.basename(stop_crossings_filepath)
    date_str = filename.replace(".parquet", "")[1:]
    service_date = datetime.strptime(date_str, "%Y%m%d").date()

    # this is the df of all useful records from the stop crossings files
    raw_stop_crossings = (
        pl.scan_parquet(stop_crossings_filepath)
        .filter(
            pl.col("ACT_ARRIVAL_TIME").is_not_null()
            | pl.col("ACT_DEPARTURE_TIME").is_not_null()
        )
        .collect()
    )

    # run the generate tm events function on our input files
    bus_events = generate_tm_events(tm_files=[stop_crossings_filepath])

    # ensure data has been extracted from the filepath
    assert not bus_events.is_empty()

    # make sure we only have a single service date and it matches the filename service date
    assert set(bus_events["service_date"]) == {service_date}

    # ensure we didn't lose any data from the raw dataset when joining
    assert len(bus_events) == len(raw_stop_crossings)

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

    # check that all arrival and departure timestamps happen after the start of the service date
    assert bus_events.filter(
        (pl.col("arrival_tm") < service_date)
        | (pl.col("departure_tm") < service_date)
    ).is_empty()

    # check that all departure times are after the arrival times
    assert bus_events.filter(
        pl.col("arrival_tm") > pl.col("departure_tm")
    ).is_empty()

    # check that there are no leading zeros on route ids
    assert bus_events.filter(
        pl.col("route_id").str.starts_with("0")
        | pl.col("trip_id").str.starts_with("0")
        | pl.col("stop_id").str.starts_with("0")
    ).is_empty()
