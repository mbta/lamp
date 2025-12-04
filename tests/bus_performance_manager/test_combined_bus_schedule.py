from contextlib import nullcontext

import pytest
import dataframely as dy
import polars as pl
from dataframely.exc import ValidationError

from lamp_py.bus_performance_manager.events_gtfs_schedule import GTFSBusSchedule
from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterPatternGeoNodeXref, TransitMasterSchedule
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedSchedule, join_tm_schedule_to_gtfs_schedule


class TestCombinedSchedule(CombinedSchedule):
    """Edge case checks that would be expensive to run in production."""

    @dy.rule()
    def consecutive_null_stop_sequences_ordered_correctly() -> pl.Expr:  # pylint: disable=no-method-argument
        """Order smaller gtfs_stop_sequences before larger gtfs_stop_sequences."""
        return pl.col("gtfs_stop_sequence").gt(
            pl.col("gtfs_stop_sequence").shift().over(partition_by="trip_id", order_by="stop_sequence")
        )

    @dy.rule()
    def first_stop_sequence_has_gtfs_record() -> pl.Expr:  # pylint: disable=no-method-argument
        """Order the non-null GTFS record first."""
        return (
            pl.col("gtfs_stop_sequence").is_null().all().over("trip_id")  # either the trip has no GTFS records at all
            | pl.col("stop_sequence").gt(1)  # the stop sequence is greater than 1
            | pl.col("gtfs_stop_sequence").is_not_null()  # or the gtfs_stop_sequence is not null
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
    df = CombinedSchedule.sample(3, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
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
    df = CombinedSchedule.sample(4, generator=dy_gen).with_columns(
        trip_id=pl.lit("1"),
        gtfs_stop_sequence=pl.Series(values=gtfs_stop_sequence),  # out of order
        tm_stop_sequence=pl.Series(values=tm_stop_sequence),
        stop_sequence=pl.Series(values=range(1, 5)),
    )

    with expected_rows:
        assert TestCombinedSchedule.validate(df, cast=True).height == expected_rows.enter_result  # type: ignore[attr-defined]
