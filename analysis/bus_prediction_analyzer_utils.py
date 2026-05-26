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

