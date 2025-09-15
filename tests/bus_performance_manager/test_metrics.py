from datetime import datetime

import dataframely as dy
import pytest

from lamp_py.bus_performance_manager.events_tm import TransitMasterEvents
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import GTFSEvents
from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
from lamp_py.bus_performance_manager.events_metrics import enrich_bus_performance_metrics, BusPerformanceMetrics

@pytest.fixture(name = "rng")
def fixture_rng(seed: int = 1) -> dy.random.Generator:
    """
    Returns random data generator using seed of parameter.
    
    :param seed:
    :type seed: int
    """
    return dy.random.Generator(seed)

@pytest.fixture(name = "bus_metrics_dataframes", params = [100])
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
