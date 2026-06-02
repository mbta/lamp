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
        # bin start, bin end, early threshold, late threshold (all in seconds)
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
    "vehicle.timestamp": "vp_timestamp",
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
    "trip_update.trip.start_time": "start_time",
    "trip_update.timestamp": "tu_timestamp",
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


def assign_ibi_bin(df: pl.DataFrame, config: AnalyzerConfig = default_config()) -> pl.DataFrame:
    """Add an ``ibi_bin`` column based on ``prediction_ahead_sec``.

    Only predictions made *before* the predicted arrival are binned
    (prediction_ahead_sec <= 0).  The negated value gives "seconds until"
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


def is_prediction_accurate(df: pl.DataFrame, config: AnalyzerConfig = default_config()) -> pl.DataFrame:
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


def calculate_ibi_accuracy(df: pl.DataFrame, config: AnalyzerConfig = default_config()) -> pl.DataFrame:
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


def assign_time_of_day_bin(df: pl.DataFrame, config: AnalyzerConfig = default_config()) -> pl.DataFrame:
    """Add a ``time_of_day_bin`` column based on trip ``start_time``.

    ``start_time`` is expected in GTFS ``HH:MM:SS`` format where hours may
    exceed 24 for overnight service (e.g., ``25:30:00``).  We compute seconds
    after midnight as ``HH*3600 + MM*60 + SS`` and assign bins using configured
    time-of-day windows by clock-time (modulo 24h).

    Bin ranges are ``[start_hour, end_hour)`` in hours and support wrap-around
    windows when ``start_hour > end_hour``.
    """
    parts = pl.col("start_time").str.split_exact(":", 2)
    start_seconds = (
        parts.struct.field("field_0").cast(pl.Int64, strict=False) * 3600
        + parts.struct.field("field_1").cast(pl.Int64, strict=False) * 60
        + parts.struct.field("field_2").cast(pl.Int64, strict=False)
    )
    seconds_in_day = start_seconds % 86400
    expr = pl.lit(None, dtype=pl.Utf8)
    for b in reversed(config.time_of_day_bins):
        start_sec = b.start_hour * 3600
        end_sec = b.end_hour * 3600
        if b.start_hour < b.end_hour:
            cond = seconds_in_day.ge(start_sec) & seconds_in_day.lt(end_sec)
        else:
            cond = seconds_in_day.ge(start_sec) | seconds_in_day.lt(end_sec)
        expr = pl.when(cond).then(pl.lit(b.name)).otherwise(expr)
    return df.with_columns(time_of_day_bin=expr)


def parse_start_time_seconds(df: pl.DataFrame) -> pl.DataFrame:
    """Add a ``start_time_seconds`` column: seconds after midnight from ``start_time``.

    Parses GTFS ``start_time`` (HH:MM:SS, allowing HH > 24) to integer seconds.
    Returns the raw value without modulo, so overnight service times remain > 86400.
    Useful for temporal binning, visualizations, and downstream metrics.
    """
    parts = pl.col("start_time").str.split_exact(":", 2)
    return df.with_columns(
        start_time_seconds=(
            parts.struct.field("field_0").cast(pl.Int64, strict=False) * 3600
            + parts.struct.field("field_1").cast(pl.Int64, strict=False) * 60
            + parts.struct.field("field_2").cast(pl.Int64, strict=False)
        )
    )


def filter_predictions(
    df: pl.DataFrame,
    route_id: str | None = None,
    trip_id: str | None = None,
    stop_id: str | None = None,
    service_date: str | None = None,
) -> pl.DataFrame:
    """Filter predictions by optional route, trip, stop, or service_date.

    Each filter is applied only if not None. Returns input DataFrame if all filters are None.
    """
    result = df
    if route_id is not None:
        result = result.filter(pl.col("route_id") == route_id)
    if trip_id is not None:
        result = result.filter(pl.col("trip_id") == trip_id)
    if stop_id is not None:
        result = result.filter(pl.col("stop_id") == stop_id)
    if service_date is not None:
        result = result.filter(pl.col("service_date") == service_date)
    return result


def calculate_accuracy_by_group(
    df: pl.DataFrame,
    group_cols: list[str],
    config: AnalyzerConfig = default_config(),
) -> pl.DataFrame:
    """Calculate per-group IBI accuracy for arbitrary grouping columns.

    Groups by ``group_cols``, calculates per-group IBI accuracy (total, accurate, accuracy_pct).
    Only rows with non-null ``ibi_bin`` are included. Returns sorted dataframe by group_cols.
    """
    binned = df.filter(pl.col("ibi_bin").is_not_null())
    if binned.is_empty():
        # Return empty dataframe with expected schema
        return pl.DataFrame(
            schema={
                **{col: pl.Utf8 for col in group_cols},
                "total": pl.UInt32,
                "accurate": pl.UInt32,
                "accuracy_pct": pl.Float64,
            }
        )

    grouped = (
        binned.group_by(group_cols)
        .agg(
            total=pl.len(),
            accurate=pl.col("is_accurate").sum(),
        )
        .with_columns(accuracy_pct=pl.col("accurate") / pl.col("total") * 100)
        .sort(group_cols)
    )
    return grouped


def detect_prediction_bias(
    df: pl.DataFrame,
    bias_threshold_sec: float = 30.0,
    consistency_threshold_sec: float = 20.0,
) -> pl.DataFrame:
    """Detect consistent directional bias per trip.

    Computes per-trip mean and std of ``prediction_error_sec``. Flags trips as biased
    when ``|mean_error| > bias_threshold_sec`` AND ``std_error < consistency_threshold_sec``
    (indicating consistent direction without adjustment).

    Args:
        df: DataFrame with trip_id and prediction_error_sec columns.
        bias_threshold_sec: Minimum |mean_error| to consider as biased (default 30.0).
        consistency_threshold_sec: Maximum std for consistent behavior (default 20.0).

    Returns:
        DataFrame with columns: trip_id, mean_error, std_error, is_biased.
    """
    trips_with_errors = df.filter(pl.col("prediction_error_sec").is_not_null())
    if trips_with_errors.is_empty():
        return pl.DataFrame(
            schema={
                "trip_id": pl.Utf8,
                "mean_error": pl.Float64,
                "std_error": pl.Float64,
                "is_biased": pl.Boolean,
            }
        )

    result = (
        trips_with_errors.group_by("trip_id")
        .agg(
            mean_error=pl.col("prediction_error_sec").mean(),
            median_error=pl.col("prediction_error_sec").median(),
            mode_error=pl.col("prediction_error_sec").mode(),
            std_error=pl.col("prediction_error_sec").std(),
        )
        .with_columns(
            std_error=pl.col("std_error").fill_null(0.0)  # Single value -> std=NaN becomes 0
        )
        .with_columns(
            is_biased=(
                (pl.col("mean_error").abs() > bias_threshold_sec)
                & (pl.col("std_error") < consistency_threshold_sec)
            )
        )
        .sort("trip_id")
    )
    return result


def run_analysis(
    tu_df: pl.DataFrame,
    vp_df: pl.DataFrame,
    config: AnalyzerConfig = default_config(),
) -> dict[str, pl.DataFrame]:
    """Orchestrate the full bus prediction analysis pipeline.

    Runs all processing steps in sequence:
    1. Narrow VP and TU to canonical columns
    2. Join TU and VP
    3. Add error columns
    4. Assign IBI bins
    5. Flag accurate predictions
    6. Calculate IBI accuracy (overall)
    7. Assign time-of-day bins
    8. Parse start_time seconds
    9. Calculate accuracy by group (route, time_of_day, ibi_bin)
    10. Detect prediction bias per trip

    Args:
        tu_df: Raw trip_updates DataFrame
        vp_df: Raw vehicle_positions DataFrame
        config: AnalyzerConfig with bins and thresholds

    Returns:
        Dict with keys:
        - 'joined': Full joined dataframe with all computed columns
        - 'ibi_accuracy': Overall IBI accuracy by bin
        - 'route_accuracy': Accuracy grouped by route_id
        - 'time_of_day_accuracy': Accuracy grouped by time_of_day_bin
        - 'bias': Prediction bias per trip
    """
    # Step 1-2: Narrow and join
    tu_narrow = narrow_trip_updates(tu_df)
    vp_narrow = narrow_vehicle_positions(vp_df)
    joined = join_tu_vp(tu_narrow, vp_narrow)

    # Step 3: Add error columns
    with_errors = add_error_columns(joined)

    # Step 4-5: IBI binning and accuracy
    with_ibi_bin = assign_ibi_bin(with_errors, config)
    with_accuracy = is_prediction_accurate(with_ibi_bin, config)

    # Step 6: Overall IBI accuracy
    ibi_accuracy = calculate_ibi_accuracy(with_accuracy, config)

    # Step 7-8: Time-of-day binning and start_time parsing
    with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    with_start_time = parse_start_time_seconds(with_tod_bin)

    # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    route_accuracy = calculate_accuracy_by_group(with_start_time, ["route_id"], config)
    time_of_day_accuracy = calculate_accuracy_by_group(
        with_start_time, ["time_of_day_bin"], config
    )

    # Step 10: Prediction bias
    bias = detect_prediction_bias(with_start_time)

    return {
        "joined": with_start_time,
        "ibi_accuracy": ibi_accuracy,
        "route_accuracy": route_accuracy,
        "time_of_day_accuracy": time_of_day_accuracy,
        "bias": bias,
    }



