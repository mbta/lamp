from contextlib import nullcontext
from datetime import datetime, date

import polars as pl
import pytest
from dataframely.random import Generator
from dataframely.exc import ValidationError
from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics


@pytest.mark.parametrize(
    ["stop_arrival_dt", "stop_departure_dt", "num_rows"],
    [
        (datetime(2000, 1, 1), datetime(2000, 1, 1), nullcontext(1)),
        (datetime(2000, 1, 1), datetime(2000, 1, 1, 1), nullcontext(1)),
        (datetime(2000, 1, 1, 1), datetime(2000, 1, 1), pytest.raises(ValidationError)),
        (None, datetime(2000, 1, 1), nullcontext(1)),
        (datetime(2000, 1, 1), None, nullcontext(1)),
    ],
    ids=[
        "departure_equal_arrival",
        "departure_after_arrival",
        "arrival_after_departure",
        "arrival_null",
        "departure_null",
    ],
)
def test_dy_departure_after_arrival(
    dy_gen: Generator, stop_arrival_dt: datetime, stop_departure_dt: datetime, num_rows: pytest.RaisesExc
) -> None:
    "It returns false if the departure dt is earlier than the arrival dt."
    df = BusPerformanceMetrics.sample(num_rows=1, generator=dy_gen).with_columns(
        stop_arrival_dt=stop_arrival_dt,
        stop_departure_dt=stop_departure_dt,
    )

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ["stop_arrival_dt", "stop_departure_dt", "service_date", "num_rows"],
    [
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [datetime(2000, 1, 1, 2), None],
            [date(2000, 1, 1), date(2000, 1, 1)],
            pytest.raises(ValidationError),
        ),
        (
            [datetime(2000, 1, 1), None],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [date(2000, 1, 1), date(2000, 1, 1)],
            pytest.raises(ValidationError),
        ),
        (
            [datetime(2000, 1, 1), datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1, 3)],
            [date(2000, 1, 1), date(2000, 1, 1)],
            nullcontext(2),
        ),
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [datetime(2000, 1, 1, 2), None],
            [date(2000, 1, 1), date(2000, 1, 2)],
            nullcontext(2),
        ),
    ],
    ids=[
        "out-of-order-arrival",
        "out-of-order_departure",
        "valid-arrival_equal-departure",
        "different_service-dates",
    ],
)
def test_dy_stop_sequence_implies_time_order(
    dy_gen: Generator,
    stop_arrival_dt: list[datetime],
    stop_departure_dt: list[datetime],
    service_date: list[date],
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if any departure or arrival time is earlier than the preceding record."
    df = BusPerformanceMetrics.sample(
        num_rows=2,
        overrides={
            "vehicle_label": ["123", "123"],
            "stop_sequence": [2, 3],
            "service_date": service_date,
            "stop_arrival_dt": stop_arrival_dt,
            "stop_departure_dt": stop_departure_dt,
        },
        generator=dy_gen,
    ).with_columns(trip_id=pl.lit("abc"))

    with num_rows:
        assert BusPerformanceMetrics.validate(df).height == num_rows.enter_result  # type: ignore[attr-defined]
