# pylint: disable=too-many-positional-arguments,too-many-arguments
from contextlib import nullcontext
from datetime import date, datetime

import polars as pl
import pytest
from dataframely.random import Generator
from dataframely.exc import ValidationError

from lamp_py.bus_performance_manager.events_joined import BusEvents


@pytest.mark.parametrize(
    ["point_type", "gtfs_last_in_transit_dt", "gtfs_arrival_dt", "num_rows"],
    [
        (
            "mid",
            datetime(2000, 1, 1, 2),
            None,
            nullcontext(3),
        ),
        (
            "end",
            None,
            None,
            nullcontext(3),
        ),
        (
            "end",
            datetime(2000, 1, 1, 2),
            None,
            pytest.raises(ValidationError, match="final_stop_has_arrival_dt"),
        ),
        (
            "end",
            datetime(2000, 1, 1, 2),
            datetime(2000, 1, 1, 2),
            nullcontext(3),
        ),
    ],
    ids=[
        "not-last-stop",  # pass
        "missing-GTFS-stop-data",  # pass
        "final-stop-but-no-gtfs-arrival",  # fail
        "all-data-present",  # pass
    ],
)
def test_dy_final_stop_has_arrival_dt(
    dy_gen: Generator,
    point_type: str,
    gtfs_last_in_transit_dt: datetime,
    gtfs_arrival_dt: list[datetime],
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if the last TM stop or, if not available, GTFS stop, has in-transit data but not a gtfs_arrival_dt."
    df = BusEvents.sample(num_rows=3, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        tm_pullout_id=pl.lit("0"),
        route_id=pl.lit("a"),
        vehicle_label=pl.lit("x"),
        service_date=pl.lit(date(2000, 1, 1)),
        tm_stop_sequence=pl.Series(values=[1, 2, 3]),
        stop_sequence=pl.Series(values=[1, 2, 3]),
        point_type=pl.Series(values=["start", "mid", point_type]),
        gtfs_last_in_transit_dt=pl.Series(
            values=[datetime(2000, 1, 1), datetime(2000, 1, 1, 1), gtfs_last_in_transit_dt]
        ),
        gtfs_arrival_dt=pl.Series(values=[datetime(2000, 1, 1), datetime(2000, 1, 1, 1), gtfs_arrival_dt]),
    )

    with num_rows:
        assert BusEvents.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ["passes", "tm_stop_sequence"],
    [
        (pytest.raises(ValidationError, match="monotonic_tm_stop_sequence"), [1, 2, 1]),
        (nullcontext(True), [1, 2, 3]),
        (nullcontext(True), [None, None, None]),
    ],
    ids=["decreasing", "increasing", "null"],
)
def test_dy_monotonic_tm_stop_sequence(
    dy_gen: Generator, passes: pytest.RaisesExc, tm_stop_sequence: list[int]
) -> None:
    """It returns false if tm_stop_sequence decreases over the stop_sequence."""
    df = BusEvents.sample(
        num_rows=3,
        generator=dy_gen,
        overrides={
            "trip_id": ["1", "1", "1"],
            "tm_pullout_id": "0",
            "vehicle_label": "y1",
            "route_id": "a",
            "service_date": date(2025, 1, 1),
        },
    ).with_columns(
        stop_sequence=pl.Series(values=[1, 2, 3]),
        tm_stop_sequence=pl.Series(values=tm_stop_sequence),
    )

    with passes:
        BusEvents.validate(df, cast=True)
