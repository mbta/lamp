# pylint: disable=too-many-positional-arguments
from contextlib import nullcontext
from datetime import datetime, date

import polars as pl
import pytest
from dataframely.random import Generator
from dataframely.exc import ValidationError
from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics


@pytest.mark.parametrize(
    ["stop_arrival_dt", "stop_departure_dt", "travel_time_seconds", "stopped_duration_seconds", "num_rows"],
    [
        (datetime(2000, 1, 1), datetime(2000, 1, 1), None, 0, nullcontext(1)),
        (datetime(2000, 1, 1), datetime(2000, 1, 1, 1), None, 60 * 60, nullcontext(1)),
        (
            datetime(2000, 1, 1, 1),
            datetime(2000, 1, 1),
            None,
            None,
            pytest.raises(ValidationError, match="departure_after_arrival"),
        ),
        (None, datetime(2000, 1, 1), None, None, nullcontext(1)),
        (datetime(2000, 1, 1), None, None, None, nullcontext(1)),
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
    dy_gen: Generator,
    stop_arrival_dt: datetime,
    stop_departure_dt: datetime,
    travel_time_seconds: int | None,
    stopped_duration_seconds: int | None,
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if the departure dt is earlier than the arrival dt."
    df = BusPerformanceMetrics.sample(num_rows=1, generator=dy_gen).with_columns(
        stop_arrival_dt=stop_arrival_dt,
        stop_departure_dt=stop_departure_dt,
        travel_time_seconds=travel_time_seconds,
        stopped_duration_seconds=stopped_duration_seconds,
    )

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    [
        "stop_arrival_dt",
        "stop_departure_dt",
        "service_date",
        "travel_time_seconds",
        "stopped_duration_seconds",
        "num_rows",
    ],
    [
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [datetime(2000, 1, 1, 2), None],
            [date(2000, 1, 1), date(2000, 1, 1)],
            [None, None],
            [None, None],
            pytest.raises(ValidationError, match="stop_sequence_implies_arrival_order"),
        ),
        (
            [datetime(2000, 1, 1), None],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [date(2000, 1, 1), date(2000, 1, 1)],
            [None, None],
            [None, None],
            pytest.raises(ValidationError, match="stop_sequence_implies_departure_order"),
        ),
        (
            [datetime(2000, 1, 1), datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1, 3)],
            [date(2000, 1, 1), date(2000, 1, 1)],
            [None, 60 * 60],
            [60 * 60, 60 * 60],
            nullcontext(2),
        ),
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1)],
            [datetime(2000, 1, 1, 2), None],
            [date(2000, 1, 1), date(2000, 1, 2)],
            [None, None],
            [60 * 60, None],
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
    travel_time_seconds: list[int],
    stopped_duration_seconds: list[int],
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if any departure or arrival time is earlier than the preceding record."
    df = BusPerformanceMetrics.sample(
        num_rows=2,
        generator=dy_gen,
    ).with_columns(
        trip_id=pl.lit("abc"),
        vehicle_label=pl.lit("123"),
        stop_sequence=pl.Series(values=[2, 3]),
        service_date=pl.Series(values=service_date),
        stop_arrival_dt=pl.Series(values=stop_arrival_dt),
        stop_departure_dt=pl.Series(values=stop_departure_dt),
        travel_time_seconds=pl.Series(values=travel_time_seconds),
        stopped_duration_seconds=pl.Series(values=stopped_duration_seconds),
    )

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ["stop_arrival_dt", "stop_departure_dt", "travel_time_seconds", "stopped_duration_seconds", "num_rows"],
    [
        (
            [None, datetime(2000, 1, 1, 1, 30), datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1, 1, 30), None],
            [None, 30 * 60, 30 * 60],
            [None, 0, None],
            nullcontext(3),
        ),
        (
            [None, None, datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1), None, None],
            [None, None, 60 * 60],
            [None, None, None],
            nullcontext(3),
        ),
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1, 1, 29), datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1, 1), datetime(2000, 1, 1, 1, 30), datetime(2000, 1, 1, 2)],
            [None, 1000 * 60, 30 * 60],
            [1 * 60, 1 * 60, 0],
            pytest.raises(ValidationError, match="travel_time_plus_stopped_duration_equals_total_trip"),
        ),
        (
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 1, 1, 29), datetime(2000, 1, 1, 2)],
            [datetime(2000, 1, 1, 1, 1), datetime(2000, 1, 1, 1, 30), datetime(2000, 1, 1, 2)],
            [None, 0, 30 * 60],
            [1 * 60, 1 * 60, 0],
            pytest.raises(ValidationError, match="travel_time_plus_stopped_duration_equals_total_trip"),
        ),
    ],
    ids=[
        "no_stopped_duration",
        "null_travel_time",
        "trip_duration_greater_than_travel_time",
        "trip_duration_less_than_travel_time",
    ],
)
def test_dy_travel_time_plus_stopped_duration_equals_total_trip(
    dy_gen: Generator,
    stop_arrival_dt: list[datetime],
    stop_departure_dt: list[datetime],
    travel_time_seconds: list[int],
    stopped_duration_seconds: list[int],
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if the travel times and stopped durations don't add up to the total trip duration."
    df = BusPerformanceMetrics.sample(num_rows=3, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        vehicle_label=pl.lit("x"),
        service_date=pl.lit(date(2000, 1, 1)),
        stop_sequence=pl.Series(values=[1, 2, 3]),
        stop_arrival_dt=pl.Series(values=stop_arrival_dt),
        stop_departure_dt=pl.Series(values=stop_departure_dt),
        travel_time_seconds=pl.Series(values=travel_time_seconds),
        stopped_duration_seconds=pl.Series(values=stopped_duration_seconds),
    )

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]
