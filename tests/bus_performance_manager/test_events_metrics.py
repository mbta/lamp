from contextlib import nullcontext
from datetime import date

import pytest
import polars as pl
from dataframely.random import Generator
from dataframely.exc import ValidationError
from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics


@pytest.mark.parametrize(
    ["point_type", "is_full_trip", "num_rows"],
    [
        (["STARTPOINT", "MIDPOINT", "ENDPOINT"], True, nullcontext(3)),
        (["STARTPOINT", "STARTPOINT", "ENDPOINT"], True, pytest.raises(ValidationError)),
        (["STARTPOINT", "ENDPOINT", "ENDPOINT"], True, pytest.raises(ValidationError)),
        (["STARTPOINT", "STARTPOINT", "ENDPOINT"], False, nullcontext(3))
    ],
    ids=[
        "unique_terminals",
        "duplicate_startpoints",
        "duplicate_endpoints",
        "not_full_trip",
    ],
)
def test_dy_departure_after_arrival(dy_gen: Generator, point_type: list[str], is_full_trip: bool, num_rows: pytest.RaisesExc) -> None:
    "It returns false if the departure dt is earlier than the arrival dt."
    df = BusPerformanceMetrics.sample(
        num_rows=3,
        generator=dy_gen,
        overrides={
            "vehicle_label": ["a", "a", "a"],
            "service_date": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "point_type": point_type,
        },
    ).with_columns(trip_id=pl.lit("1"), is_full_trip=pl.lit(is_full_trip))

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]
