"""Bus prediction analyzer utility functions.

Pure functions for analyzing GTFS-RT trip_update predictions
against vehicle_position ground truth using the IBI metric.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


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

