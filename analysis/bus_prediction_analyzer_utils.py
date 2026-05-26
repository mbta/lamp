"""Bus prediction analyzer utility functions.

Pure functions for analyzing GTFS-RT trip_update predictions
against vehicle_position ground truth using the IBI metric.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class IBIBin:
    """A single IBI (Itinerary-Based Indicator) time bucket."""

    name: str
    min_seconds_away: int
    max_seconds_away: int
    early_threshold_sec: float
    late_threshold_sec: float


@dataclass(frozen=True)
class TimeOfDayBin:
    """A time-of-day bin defined by start/end hour (0-23)."""

    name: str
    start_hour: int
    end_hour: int


@dataclass(frozen=True)
class AnalyzerConfig:
    """Immutable configuration for the bus prediction analyzer."""

    ibi_bins: tuple[IBIBin, ...] = field(default_factory=tuple)
    time_of_day_bins: tuple[TimeOfDayBin, ...] = field(default_factory=tuple)
    ignore_threshold_sec: int = 900


def default_config() -> AnalyzerConfig:
    """Return the default analyzer config with TransitApp IBI spec bins."""
    return AnalyzerConfig(
        ibi_bins=(
            IBIBin("0-3min", 0, 180, 30, 90),
            IBIBin("3-6min", 180, 360, 60, 150),
            IBIBin("6-10min", 360, 600, 60, 210),
            IBIBin("10-15min", 600, 900, 90, 270),
        ),
        time_of_day_bins=(
            TimeOfDayBin("late_night", 0, 6),
            TimeOfDayBin("morning_rush", 6, 9),
            TimeOfDayBin("midday", 9, 16),
            TimeOfDayBin("evening_rush", 16, 19),
            TimeOfDayBin("night", 19, 24),
        ),
        ignore_threshold_sec=900,
    )


def load_config(path: str | Path) -> AnalyzerConfig:
    """Load an AnalyzerConfig from a JSON file."""
    with open(path) as f:
        raw = json.load(f)

    ibi_bins = tuple(IBIBin(**b) for b in raw.get("ibi_bins", []))
    time_of_day_bins = tuple(TimeOfDayBin(**b) for b in raw.get("time_of_day_bins", []))
    ignore_threshold_sec = raw.get("ignore_threshold_sec", 900)

    return AnalyzerConfig(
        ibi_bins=ibi_bins,
        time_of_day_bins=time_of_day_bins,
        ignore_threshold_sec=ignore_threshold_sec,
    )


# Column name mappings: GTFS-RT raw -> canonical
VP_COLUMN_MAP = {
    "vehicle.trip.trip_id": "trip_id",
    "vehicle.current_stop_sequence": "stop_sequence",
    "vehicle.vehicle.id": "vehicle_id",
    "vehicle.timestamp": "actual_timestamp",
    "vehicle.current_status": "current_status",
    "feed_timestamp": "vp_feed_timestamp",
}


def narrow_vehicle_positions(df: pl.DataFrame) -> pl.DataFrame:
    """Filter and deduplicate vehicle positions to one row per stop visit.

    Filters to STOPPED_AT status, deduplicates by (trip_id, stop_sequence)
    keeping the first occurrence, renames to canonical column names, and sorts.
    """
    required = list(VP_COLUMN_MAP.keys())
    return (
        df.select(required)
        .filter(pl.col("vehicle.current_status") == "STOPPED_AT")
        .unique(subset=["vehicle.trip.trip_id", "vehicle.current_stop_sequence"], keep="first")
        .sort("vehicle.trip.trip_id", "vehicle.current_stop_sequence")
        .rename(VP_COLUMN_MAP)
        .drop("current_status")
    )


TU_COLUMN_MAP = {
    "trip_update.trip.trip_id": "trip_id",
    "trip_update.stop_time_update.stop_sequence": "stop_sequence",
    "trip_update.stop_time_update.stop_id": "stop_id",
    "trip_update.vehicle.id": "vehicle_id",
    "trip_update.stop_time_update.arrival.time": "predicted_arrival",
    "trip_update.trip.route_id": "route_id",
    "feed_timestamp": "tu_feed_timestamp",
}


def narrow_trip_updates(df: pl.DataFrame) -> pl.DataFrame:
    """Select and rename trip_update columns to canonical names."""
    required = list(TU_COLUMN_MAP.keys())
    return df.select(required).rename(TU_COLUMN_MAP)


def join_tu_vp(tu_df: pl.DataFrame, vp_df: pl.DataFrame) -> pl.DataFrame:
    """Left join narrowed trip_updates onto narrowed vehicle_positions.

    Join keys: trip_id, vehicle_id, stop_sequence.
    """
    return tu_df.join(
        vp_df,
        on=["trip_id", "vehicle_id", "stop_sequence"],
        how="left",
    )


def add_error_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add prediction error and prediction-ahead columns.

    prediction_error_sec: predicted_arrival - actual_timestamp
        positive = predicted too late, negative = predicted too early
    prediction_ahead_sec: tu_feed_timestamp - predicted_arrival
        negative = prediction was made before the predicted arrival
    """
    return df.with_columns(
        prediction_error_sec=pl.col("predicted_arrival") - pl.col("actual_timestamp"),
        prediction_ahead_sec=pl.col("tu_feed_timestamp") - pl.col("predicted_arrival"),
    )


def assign_ibi_bin(df: pl.DataFrame, config: AnalyzerConfig) -> pl.DataFrame:
    """Add an ``ibi_bin`` column based on ``prediction_ahead_sec``.

    Only predictions made *before* the predicted arrival are binned
    (prediction_ahead_sec <= 0).  The negated value gives "seconds until
    arrival" which is mapped into the configured IBI bin ranges.
    Positive prediction_ahead_sec (stale / after-the-fact) and values
    outside all bins receive null.

    Bin ranges are ``[min_seconds_away, max_seconds_away)``.
    """
    seconds_until = -pl.col("prediction_ahead_sec")
    expr = pl.lit(None, dtype=pl.Utf8)
    for b in reversed(config.ibi_bins):
        expr = (
            pl.when(seconds_until.ge(b.min_seconds_away) & seconds_until.lt(b.max_seconds_away))
            .then(pl.lit(b.name))
            .otherwise(expr)
        )
    return df.with_columns(ibi_bin=expr)


def is_prediction_accurate(df: pl.DataFrame, config: AnalyzerConfig) -> pl.DataFrame:
    """Add boolean ``is_accurate`` column using IBI bin thresholds.

    A prediction is accurate when:
      -early_threshold_sec <= prediction_error_sec <= late_threshold_sec

    Rows with null ``ibi_bin`` receive null ``is_accurate``.
    Thresholds are inclusive at both boundaries per TransitApp spec.
    """
    error = pl.col("prediction_error_sec")
    expr = pl.lit(None, dtype=pl.Boolean)
    for b in reversed(config.ibi_bins):
        expr = (
            pl.when(pl.col("ibi_bin") == b.name)
            .then(error.ge(-b.early_threshold_sec) & error.le(b.late_threshold_sec))
            .otherwise(expr)
        )
    return df.with_columns(is_accurate=expr)


def calculate_ibi_accuracy(df: pl.DataFrame, config: AnalyzerConfig) -> pl.DataFrame:
    """Calculate per-bin and overall IBI accuracy.

    Returns a DataFrame with columns: ibi_bin, total, accurate, accuracy_pct.
    The overall row is the equal-weighted average of per-bin accuracies.
    Bins with no data are excluded from the overall average.
    """
    binned = df.filter(pl.col("ibi_bin").is_not_null())
    if binned.is_empty():
        return pl.DataFrame(
            schema={"ibi_bin": pl.Utf8, "total": pl.UInt32, "accurate": pl.UInt32, "accuracy_pct": pl.Float64}
        )

    per_bin = (
        binned.group_by("ibi_bin")
        .agg(
            total=pl.len(),
            accurate=pl.col("is_accurate").sum(),
        )
        .with_columns(accuracy_pct=pl.col("accurate") / pl.col("total") * 100)
        .sort("ibi_bin")
    )

    overall_acc = per_bin["accuracy_pct"].mean()
    overall_row = pl.DataFrame(
        {
            "ibi_bin": ["overall"],
            "total": [per_bin["total"].sum()],
            "accurate": [per_bin["accurate"].sum()],
            "accuracy_pct": [overall_acc],
        },
        schema={"ibi_bin": pl.Utf8, "total": pl.UInt32, "accurate": pl.UInt32, "accuracy_pct": pl.Float64},
    )
    return pl.concat([per_bin, overall_row])

