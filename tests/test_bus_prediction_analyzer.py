"""Tests for bus_prediction_analyzer_utils — Step 1: Config."""

import json
from pathlib import Path

import pytest

from analysis.bus_prediction_analyzer_utils import (
    AnalyzerConfig,
    IBIBin,
    TimeOfDayBin,
    default_config,
    load_config,
)


class TestDefaultConfig:
    """Tests for default_config()."""

    def test_returns_analyzer_config(self):
        cfg = default_config()
        assert isinstance(cfg, AnalyzerConfig)

    def test_has_four_ibi_bins(self):
        cfg = default_config()
        assert len(cfg.ibi_bins) == 4

    def test_has_five_time_of_day_bins(self):
        cfg = default_config()
        assert len(cfg.time_of_day_bins) == 5

    @pytest.mark.parametrize(
        ["bin_index", "expected_name", "expected_min", "expected_max", "expected_early", "expected_late"],
        [
            (0, "0-3min", 0, 180, 30, 90),
            (1, "3-6min", 180, 360, 60, 150),
            (2, "6-10min", 360, 600, 60, 210),
            (3, "10-15min", 600, 900, 90, 270),
        ],
        ids=["0-3min", "3-6min", "6-10min", "10-15min"],
    )
    def test_ibi_bin_values(self, bin_index, expected_name, expected_min, expected_max, expected_early, expected_late):
        cfg = default_config()
        b = cfg.ibi_bins[bin_index]
        assert b.name == expected_name
        assert b.min_seconds_away == expected_min
        assert b.max_seconds_away == expected_max
        assert b.early_threshold_sec == expected_early
        assert b.late_threshold_sec == expected_late

    @pytest.mark.parametrize(
        ["bin_index", "expected_name", "expected_start", "expected_end"],
        [
            (0, "late_night", 0, 6),
            (1, "morning_rush", 6, 9),
            (2, "midday", 9, 16),
            (3, "evening_rush", 16, 19),
            (4, "night", 19, 24),
        ],
        ids=["late_night", "morning_rush", "midday", "evening_rush", "night"],
    )
    def test_time_of_day_bin_values(self, bin_index, expected_name, expected_start, expected_end):
        cfg = default_config()
        b = cfg.time_of_day_bins[bin_index]
        assert b.name == expected_name
        assert b.start_hour == expected_start
        assert b.end_hour == expected_end

    def test_ibi_bins_are_contiguous(self):
        cfg = default_config()
        for i in range(len(cfg.ibi_bins) - 1):
            assert cfg.ibi_bins[i].max_seconds_away == cfg.ibi_bins[i + 1].min_seconds_away

    def test_config_is_frozen(self):
        cfg = default_config()
        with pytest.raises(AttributeError):
            cfg.ignore_threshold_sec = 999


class TestLoadConfig:
    """Tests for load_config()."""

    @pytest.mark.parametrize(
        ["json_data", "expected_bin_count", "expected_threshold"],
        [
            (
                {
                    "ibi_bins": [
                        {"name": "short", "min_seconds_away": 0, "max_seconds_away": 120, "early_threshold_sec": 30, "late_threshold_sec": 60},
                    ],
                    "ignore_threshold_sec": 600,
                },
                1,
                600,
            ),
            (
                {
                    "ibi_bins": [
                        {"name": "a", "min_seconds_away": 0, "max_seconds_away": 60, "early_threshold_sec": 10, "late_threshold_sec": 20},
                        {"name": "b", "min_seconds_away": 60, "max_seconds_away": 120, "early_threshold_sec": 20, "late_threshold_sec": 40},
                    ],
                    "ignore_threshold_sec": 300,
                },
                2,
                300,
            ),
            (
                {},
                0,
                900,
            ),
        ],
        ids=["single-bin", "two-bins", "empty-uses-defaults"],
    )
    def test_load_from_json_file(self, tmp_path: Path, json_data, expected_bin_count, expected_threshold):
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(json_data))

        cfg = load_config(config_path)
        assert len(cfg.ibi_bins) == expected_bin_count
        assert cfg.ignore_threshold_sec == expected_threshold

    def test_load_with_time_of_day_bins(self, tmp_path: Path):
        json_data = {
            "time_of_day_bins": [
                {"name": "am", "start_hour": 6, "end_hour": 12},
                {"name": "pm", "start_hour": 12, "end_hour": 18},
            ],
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(json_data))

        cfg = load_config(config_path)
        assert len(cfg.time_of_day_bins) == 2
        assert cfg.time_of_day_bins[0].name == "am"
        assert cfg.time_of_day_bins[1].end_hour == 18

    def test_load_nonexistent_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.json")

    def test_load_invalid_json_raises(self, tmp_path: Path):
        config_path = tmp_path / "bad.json"
        config_path.write_text("not json at all")
        with pytest.raises(json.JSONDecodeError):
            load_config(config_path)

    def test_load_missing_ibi_field_raises(self, tmp_path: Path):
        json_data = {
            "ibi_bins": [
                {"name": "bad_bin", "min_seconds_away": 0},
            ],
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(json_data))
        with pytest.raises(TypeError):
            load_config(config_path)

    def test_roundtrip_matches_default(self, tmp_path: Path):
        """Serialize default config to JSON and reload — should match."""
        cfg = default_config()
        json_data = {
            "ibi_bins": [
                {
                    "name": b.name,
                    "min_seconds_away": b.min_seconds_away,
                    "max_seconds_away": b.max_seconds_away,
                    "early_threshold_sec": b.early_threshold_sec,
                    "late_threshold_sec": b.late_threshold_sec,
                }
                for b in cfg.ibi_bins
            ],
            "time_of_day_bins": [
                {"name": b.name, "start_hour": b.start_hour, "end_hour": b.end_hour}
                for b in cfg.time_of_day_bins
            ],
            "ignore_threshold_sec": cfg.ignore_threshold_sec,
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(json_data))

        loaded = load_config(config_path)
        assert loaded == cfg
