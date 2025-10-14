from contextlib import nullcontext
from datetime import date

import pytest
import polars as pl
from dataframely.random import Generator
from dataframely.exc import ValidationError
from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics


@pytest.mark.parametrize(
    ["point_type", "num_rows"],
    [
        (["STARTPOINT", "MIDPOINT", "ENDPOINT"], nullcontext(3)),
        (["STARTPOINT", "STARTPOINT", "ENDPOINT"], pytest.raises(ValidationError)),
        (["STARTPOINT", "ENDPOINT", "ENDPOINT"], pytest.raises(ValidationError)),
    ],
    ids=[
        "unique_terminals",
        "duplicate_startpoints",
        "duplicate_endpoints",
    ],
)
def test_dy_departure_after_arrival(dy_gen: Generator, point_type: list[str], num_rows: pytest.RaisesExc) -> None:
    "It returns false if the departure dt is earlier than the arrival dt."
    df = BusPerformanceMetrics.sample(
        num_rows=3,
        generator=dy_gen,
        overrides={
            "vehicle_label": ["a", "a", "a"],
            "service_date": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "point_type": point_type,
        },
    ).with_columns(trip_id=pl.lit("1"))

    with num_rows:
        assert BusPerformanceMetrics.validate(df, cast=True).height == num_rows.enter_result  # type: ignore[attr-defined]
