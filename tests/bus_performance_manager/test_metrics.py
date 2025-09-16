from datetime import datetime, date, time, timedelta

import dataframely as dy
import polars as pl
import pytest

from lamp_py.bus_performance_manager.events_tm import TransitMasterEvents
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import GTFSEvents
from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
from lamp_py.bus_performance_manager.events_metrics import (
    enrich_bus_performance_metrics,
    BusPerformanceMetrics,
    timestamp_to_service_date,
)


@pytest.fixture(name="rng")
def fixture_rng(seed: int = 1) -> dy.random.Generator:
    """
    Returns random data generator using seed of parameter.

    :param seed:
    :type seed: int
    """
    return dy.random.Generator(seed)


@pytest.fixture(name="bus_metrics_dataframes", params=[100])
def fixture_bus_metrics_dataframes(rng: dy.random.Generator, request: pytest.FixtureRequest) -> tuple:
    "Necessary fixtures for `join_rt_to_schedule`."
    tm_events = TransitMasterEvents.sample(
        overrides={  # introduce null values into tm_events
            "trip_id": rng.sample_string(request.param, regex=r"[a-zA-Z0-9-]+"),
            "stop_id": rng.sample_string(request.param, regex=r"[a-zA-Z0-9]+"),
            "tm_actual_arrival_dt": rng.sample_datetime(
                request.param, min=datetime(2022, 1, 1), max=None, time_zone="UTC", null_probability=0.2
            ),
            "vehicle_label": rng.sample_string(request.param, regex=r"[a-zA-Z0-9_]+"),
        },
    )

    gtfs_events = GTFSEvents.sample(
        overrides={
            "trip_id": tm_events["trip_id"],
            "stop_id": tm_events["stop_id"],
            "vehicle_label": tm_events["vehicle_label"],
        }
    )

    combined_schedule = CombinedSchedule.sample(
        overrides={
            "trip_id": tm_events["trip_id"],
            "stop_id": tm_events["stop_id"],
            "tm_stop_sequence": tm_events["tm_stop_sequence"],
            "stop_sequence": gtfs_events["stop_sequence"],
        }
    )

    return combined_schedule, gtfs_events, tm_events


def test_preserve_non_null_tm_values(bus_metrics_dataframes: tuple) -> None:
    """
    Non-null values from TransitMaster records are the same value in the final dataframe.
    """

    bus_events = enrich_bus_performance_metrics(join_rt_to_schedule(*bus_metrics_dataframes))

    BusPerformanceMetrics.validate({"bus_events": bus_events, "tm_events": bus_metrics_dataframes[2]})


def test_timestamp_to_service_date(rng: dy.random.Generator, service_date_start_hour: int = 3) -> None:
    "It returns the correct service_date."

    test_date = rng.sample_date(min=date(2020, 1, 1), max=date.today())[0]
    test_times = pl.concat(
        [rng.sample_time(2, min=time(2, 55), max=time(3)), rng.sample_time(3, min=time(3), max=time(3, 5))]
    )

    test_df = pl.DataFrame(
        {
            "expected_date": [test_date - timedelta(1)] * 2 + [test_date] * 3,
            "test_dates": [test_date] * 5,
            "test_times": test_times,
        }
    ).with_columns(
        timestamp_to_service_date(pl.col("test_dates").dt.combine(pl.col("test_times")), service_date_start_hour).alias(
            "service_date"
        )
    )

    assert test_df.filter(pl.col("expected_date") != pl.col("service_date")).height == 0
