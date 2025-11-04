# pylint: disable=too-many-positional-arguments,too-many-arguments
from contextlib import nullcontext
from datetime import date, datetime

import polars as pl
import pytest
from dataframely.random import Generator
from dataframely.exc import ValidationError

from lamp_py.bus_performance_manager.events_joined import BusEvents


@pytest.mark.parametrize(
    ["point_type", "gtfs_stop_sequence", "plan_stop_count", "gtfs_last_in_transit_dt", "gtfs_arrival_dt", "num_rows"],
    [
        (
            "mid",
            3,
            4,
            datetime(2000, 1, 1, 2),
            None,
            nullcontext(3),
        ),
        (
            None,
            3,
            3,
            datetime(2000, 1, 1, 2),
            datetime(2000, 1, 1, 2),
            nullcontext(3),
        ),
        (
            "end",
            None,
            None,
            datetime(2000, 1, 1, 2),
            datetime(2000, 1, 1, 2),
            nullcontext(3),
        ),
        (
            "end",
            3,
            3,
            datetime(2000, 1, 1, 2),
            None,
            pytest.raises(ValidationError, match="final_stop_has_arrival_dt"),
        ),
        (
            "end",
            3,
            3,
            datetime(2000, 1, 1, 2),
            datetime(2000, 1, 1, 2),
            nullcontext(3),
        ),
    ],
    ids=[
        "not-last-stop",  # pass
        "missing-TM-stop-data",  # pass
        "missing-GTFS-stop-data",  # pass
        "final-stop-but-no-gtfs-arrival",  # fail
        "all-data-present",  # pass
    ],
)
def test_dy_final_stop_has_arrival_dt(
    dy_gen: Generator,
    point_type: str,
    gtfs_stop_sequence: list[int],
    plan_stop_count: int,
    gtfs_last_in_transit_dt: datetime,
    gtfs_arrival_dt: list[datetime],
    num_rows: pytest.RaisesExc,
) -> None:
    "It returns false if the last TM stop or, if not available, GTFS stop, has in-transit data but not a gtfs_arrival_dt."
    df = BusEvents.sample(num_rows=3, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        vehicle_label=pl.lit("x"),
        service_date=pl.lit(date(2000, 1, 1)),
        point_type=pl.Series(values=["start", "mid", point_type]),
        gtfs_stop_sequence=pl.Series(values=[1, 2, gtfs_stop_sequence]),
        plan_stop_count=pl.lit(plan_stop_count),
        gtfs_last_in_transit_dt=pl.Series(
            values=[datetime(2000, 1, 1), datetime(2000, 1, 1, 1), gtfs_last_in_transit_dt]
        ),
        gtfs_arrival_dt=pl.Series(values=[datetime(2000, 1, 1), datetime(2000, 1, 1, 1), gtfs_arrival_dt]),
    )

    with num_rows:
        assert BusEvents.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]
