from contextlib import nullcontext
from datetime import datetime
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
