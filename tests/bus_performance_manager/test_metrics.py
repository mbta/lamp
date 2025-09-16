from datetime import date, time, timedelta

import dataframely as dy
import polars as pl
import pytest

from lamp_py.bus_performance_manager.events_metrics import timestamp_to_service_date


@pytest.fixture(name="rng")
def fixture_rng(seed: int = 1) -> dy.random.Generator:
    """
    Returns random data generator using seed of parameter.

    :param seed:
    :type seed: int
    """
    return dy.random.Generator(seed)


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
