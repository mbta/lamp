from contextlib import nullcontext
from datetime import datetime, date

import pytest
import dataframely as dy
import polars as pl
from dataframely.exc import ValidationError

from lamp_py.bus_performance_manager.events_gtfs_schedule import GTFSBusSchedule
from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterSchedule
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedBusSchedule, join_tm_schedule_to_gtfs_schedule


class TestCombinedSchedule(CombinedBusSchedule):
    """Edge case checks that would be expensive to run in production."""

    @dy.rule()
    def consecutive_null_stop_sequences_ordered_correctly(cls) -> pl.Expr:
        """Order smaller gtfs_stop_sequences before larger gtfs_stop_sequences."""
        return pl.col("gtfs_stop_sequence").gt(
            pl.col("gtfs_stop_sequence")
            .shift()
            .over(partition_by=["trip_id", "tm_pullout_id"], order_by="stop_sequence")
        )

    @dy.rule()
    def first_stop_sequence_has_gtfs_record(cls) -> pl.Expr:
        """Order the non-null GTFS record first."""
        return (  # either
            (
                pl.col("gtfs_stop_sequence").eq(1) & pl.col("stop_sequence").eq(1)
            )  # gtfs stop sequence 1 is the first record
            | pl.col("gtfs_stop_sequence").is_null()  # or the gtfs_stop_sequence is null
            | pl.col("gtfs_stop_sequence").gt(1)  # or the gtfs_stop_sequence is greater than 1
        )


@pytest.mark.parametrize(
    ["gtfs_stop_sequence", "tm_stop_sequence", "expected_rows"],
    [
        ([None, 1, 2], [1, None, 2], pytest.raises(ValidationError, match="first_stop_sequence_has_gtfs_record")),
        ([1, None, 2], [None, 1, 2], nullcontext(3)),
        ([1, None, 2], [1, 2, None], nullcontext(3)),
    ],
    ids=[
        "tm-before-gts",
        "gtfs-before-tm",
        "not-stop-sequence-1",
    ],
)
def test_dy_first_stop_sequence_has_gtfs_record(
    dy_gen: dy.random.Generator,
    gtfs_stop_sequence: list[int | None],
    tm_stop_sequence: list[int | None],
    expected_rows: pytest.RaisesExc,
) -> None:
    """It filters out records with non-null TM records before non-null GTFS records at the first stop_sequence."""
    df = CombinedBusSchedule.sample(3, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        service_date=pl.datetime(2025, 1, 1),
        route_id=pl.lit("a"),
        tm_pullout_id=pl.lit("x"),
        stop_sequence=pl.Series(values=[1, 2, 3]),
        tm_stop_sequence=pl.Series(values=tm_stop_sequence),
        gtfs_stop_sequence=pl.Series(values=gtfs_stop_sequence),
    )

    with expected_rows:
        assert TestCombinedSchedule.validate(df, cast=True).height == expected_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ["gtfs_stop_sequence", "tm_stop_sequence", "expected_rows"],
    [
        ([1, 2, 3, 4], [1, 2, 3, 4], nullcontext(4)),
        (
            [1, 2, 4, 3],
            [1, 2, None, 3],
            pytest.raises(ValidationError, match="consecutive_null_stop_sequences_ordered_correctly"),
        ),
        ([None, None, None, None], [1, 2, 3, 4], nullcontext(4)),
        (
            [1, 2, 2, 3],
            [1, 2, 3, 4],
            pytest.raises(ValidationError, match="consecutive_null_stop_sequences_ordered_correctly"),
        ),
    ],
    ids=[
        "in-order",
        "out-of-order",
        "no_gtfs_stop_sequence",
        "duplicate-tm_stop_sequences",
    ],
)
def test_dy_consecutive_null_stop_sequences_ordered_correctly(
    dy_gen: dy.random.Generator,
    gtfs_stop_sequence: list[int | None],
    tm_stop_sequence: list[int | None],
    expected_rows: pytest.RaisesExc,
) -> None:
    """It filters rows that are out of order."""
    df = CombinedBusSchedule.sample(4, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        route_id=pl.lit("a"),
        tm_pullout_id=pl.lit("x"),
        service_date=pl.datetime(2025, 1, 1),
        gtfs_stop_sequence=pl.Series(values=gtfs_stop_sequence),  # out of order
        tm_stop_sequence=pl.Series(values=tm_stop_sequence),
        stop_sequence=pl.Series(values=range(1, 5)),
    )

    with expected_rows:
        assert TestCombinedSchedule.validate(df, cast=True).height == expected_rows.enter_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    [
        "gtfs_stop_sequence",
        "gtfs_stop_id",
        "gtfs_plan_stop_departure_dt",
        "raises",
    ],
    [
        (
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2), datetime(2025, 1, 1, 3), datetime(2025, 1, 1, 4)],
            nullcontext(),
        ),
        (
            [1, 2, 4, 3],
            [
                "a",
                "b",
                "d",
                "c",
            ],
            [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2), datetime(2025, 1, 1, 4), datetime(2025, 1, 1, 3)],
            nullcontext(),
        ),
        (
            [1, 2, 3],
            ["z", "b", "c"],
            [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2), datetime(2025, 1, 1, 3)],
            nullcontext(),
        ),
        (
            [1, 2, 3],
            ["a", "z", "c"],
            [datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2, 30), datetime(2025, 1, 1, 3)],
            nullcontext(),
        ),
    ],
    ids=[
        "originally-in-order",
        "originally-out-of-order",
        "startpoint-disagreement",
        "midpoint-disagreement",
    ],
)
def test_correct_sequencing(
    dy_gen: dy.random.Generator,
    gtfs_stop_sequence: list[int],
    gtfs_stop_id: list[str],
    gtfs_plan_stop_departure_dt: list[datetime],
    raises: pytest.RaisesExc,
) -> None:
    """Given out-of-order GTFS stops, it returns correctly ordered sequences."""
    trip_id = pl.lit("1")
    service_date = pl.datetime(2025, 1, 1)
    route_id = pl.lit("a")

    gtfs = GTFSBusSchedule.cast(
        GTFSBusSchedule.sample(len(gtfs_stop_id), generator=dy_gen).with_columns(
            trip_id=trip_id,
            service_date=service_date,
            route_id=route_id,
            stop_id=pl.Series(values=gtfs_stop_id),
            gtfs_stop_sequence=pl.Series(values=gtfs_stop_sequence),
            plan_stop_departure_dt=pl.Series(values=gtfs_plan_stop_departure_dt),
        )
    )

    tm_schedule = TransitMasterSchedule.cast(
        TransitMasterSchedule.sample(3, generator=dy_gen).with_columns(
            tm_stop_sequence=pl.Series(values=[1, 2, 3]),
            service_date=service_date,
            route_id=route_id,
            tm_stop_departure_dt=pl.Series(
                values=[datetime(2025, 1, 1, 1), datetime(2025, 1, 1, 2), datetime(2025, 1, 1, 3)]
            ),
            PULLOUT_ID=pl.lit("0"),
            stop_id=pl.Series(
                values=[
                    "a",
                    "b",
                    "c",
                ]
            ),
            trip_id=trip_id,
        )
    )

    with raises:
        assert not TestCombinedSchedule.validate(
            join_tm_schedule_to_gtfs_schedule(gtfs, tm_schedule), cast=True
        ).is_empty()


@pytest.mark.parametrize(
    [
        "gtfs_plan_stop_departure_dt",
        "raises",
    ],
    [
        ([datetime(2025, 1, 1, 1, 0), datetime(2025, 1, 1, 2, 0), datetime(2025, 1, 1, 3, 0)], nullcontext()),
        (
            [datetime(2025, 1, 1, 1, 30), datetime(2025, 1, 1, 2, 30), datetime(2025, 1, 1, 3, 30)],
            pytest.raises(AssertionError),
        ),
    ],
    ids=[
        "1-many-match",
        "0-matches",
    ],
)
def test_drop_overloads(
    dy_gen: dy.random.Generator, gtfs_plan_stop_departure_dt: list[datetime], raises: pytest.RaisesExc
) -> None:
    """It requires that TM trip IDs match a GTFS id."""
    trip_id = pl.lit("1")
    stop_id = ["a", "b", "c"]
    stop_sequence = [1, 2, 3]
    service_date = pl.lit(date(2025, 1, 1))

    gtfs = GTFSBusSchedule.cast(
        GTFSBusSchedule.sample(len(stop_id), generator=dy_gen).with_columns(
            trip_id=trip_id,
            stop_id=pl.Series(values=stop_id),
            gtfs_stop_sequence=pl.Series(values=stop_sequence),
            plan_stop_departure_dt=pl.Series(values=gtfs_plan_stop_departure_dt),
            service_date=service_date,
        )
    )

    tm_schedule = TransitMasterSchedule.cast(
        TransitMasterSchedule.sample(9, generator=dy_gen).with_columns(
            tm_stop_sequence=pl.Series(values=stop_sequence * 3),
            tm_stop_departure_dt=pl.Series(
                values=[
                    datetime(2025, 1, 1, 0, 59),
                    datetime(2025, 1, 1, 1, 59),
                    datetime(2025, 1, 1, 2, 59),
                    datetime(2025, 1, 1, 1, 0),
                    datetime(2025, 1, 1, 2, 0),
                    datetime(2025, 1, 1, 3, 0),
                    datetime(2025, 1, 1, 1, 1),
                    datetime(2025, 1, 1, 2, 1),
                    datetime(2025, 1, 1, 3, 1),
                ]
            ),
            stop_id=pl.Series(values=stop_id * 3),
            trip_id=trip_id,
            PULLOUT_ID=pl.Series(values=[1, 1, 1, 2, 2, 2, 3, 3, 3]),
            service_date=service_date,
        )
    )

    with raises:
        assert not TestCombinedSchedule.validate(
            join_tm_schedule_to_gtfs_schedule(gtfs, tm_schedule), cast=True
        ).is_empty()
