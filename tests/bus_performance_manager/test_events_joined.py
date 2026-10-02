# pylint: disable=too-many-positional-arguments,too-many-arguments
from contextlib import nullcontext
from datetime import date

import polars as pl
import pytest
from dataframely.random import Generator
from dataframely.exc import ValidationError

from lamp_py.bus_performance_manager.events_joined import BusEvents


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
