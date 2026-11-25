"""Regression tests for the bus schedule wall-clock convention in Tableau exports."""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions


ACTUAL_DATETIME_COLUMNS = (
    "stop_arrival_dt",
    "stop_departure_dt",
    "gtfs_first_in_transit_dt",
    "gtfs_last_in_transit_dt",
    "tm_actual_arrival_dt",
    "tm_actual_departure_dt",
    "gtfs_departure_dt",
    "gtfs_arrival_dt",
)


@pytest.mark.parametrize(
    "service_date,scheduled_seconds",
    [
        (date(2026, 10, 2), 8 * 3600 + 49 * 60),
        (date(2026, 1, 15), 8 * 3600 + 49 * 60),
        (date(2026, 10, 2), 25 * 3600 + 30 * 60),
        (date(2026, 1, 15), 25 * 3600 + 30 * 60),
        (date(2026, 3, 8), 3 * 3600 + 30 * 60),
        (date(2026, 11, 1), 3 * 3600 + 30 * 60),
    ],
)
def test_planned_datetimes_preserve_schedule_wall_clock(service_date: date, scheduled_seconds: int) -> None:
    """Keep planned wall-clock values while converting actual UTC instants to Eastern."""
    midnight = datetime.combine(service_date, datetime.min.time())
    planned = midnight + timedelta(seconds=scheduled_seconds)
    actual = planned + timedelta(seconds=25)
    actual_utc = actual.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)

    # GTFS and TransitMaster schedule builders tag wall-clock schedule values UTC;
    # actual event timestamps, in contrast, are genuine UTC instants.
    data = {
        "service_date": pl.Series([service_date], dtype=pl.Date),
        "plan_start_time": pl.Series([scheduled_seconds], dtype=pl.Int64),
        "plan_stop_departure_sam": pl.Series([scheduled_seconds], dtype=pl.Int64),
        "plan_start_dt": pl.Series([planned.replace(tzinfo=timezone.utc)], dtype=pl.Datetime("us", "UTC")),
        "plan_stop_departure_dt": pl.Series([planned.replace(tzinfo=timezone.utc)], dtype=pl.Datetime("us", "UTC")),
    }
    for column in ACTUAL_DATETIME_COLUMNS:
        data[column] = pl.Series([actual_utc], dtype=pl.Datetime("us", "UTC"))

    converted = apply_bus_analysis_conversions(pl.DataFrame(data))

    assert converted["plan_start_dt"][0] == planned
    assert converted["plan_stop_departure_dt"][0] == planned
    assert (converted["plan_start_dt"][0] - midnight).total_seconds() == converted["plan_start_time"][0]
    assert (converted["plan_stop_departure_dt"][0] - midnight).total_seconds() == converted["plan_stop_departure_sam"][
        0
    ]
    assert (converted["stop_departure_dt"][0] - converted["plan_stop_departure_dt"][0]).total_seconds() == 25
    assert converted["stop_departure_seconds"][0] - converted["plan_stop_departure_sam"][0] == 25
    for column in ACTUAL_DATETIME_COLUMNS:
        assert converted[column][0] == actual
        assert converted.schema[column] == pl.Datetime("us")
    for column in ("plan_start_dt", "plan_stop_departure_dt"):
        assert converted.schema[column] == pl.Datetime("us")


def test_null_planned_datetimes_remain_null() -> None:
    """Preserve missing timestamps and the naive output timestamp schema."""
    data = {"service_date": pl.Series([date(2026, 10, 2)], dtype=pl.Date)}
    for column in (*ACTUAL_DATETIME_COLUMNS, "plan_start_dt", "plan_stop_departure_dt"):
        data[column] = pl.Series([None], dtype=pl.Datetime("us", "UTC"))
    converted = apply_bus_analysis_conversions(pl.DataFrame(data))
    for column in (*ACTUAL_DATETIME_COLUMNS, "plan_start_dt", "plan_stop_departure_dt"):
        assert converted[column][0] is None
        assert converted.schema[column] == pl.Datetime("us")
