"""Tests for bus_prediction_analyzer_utils."""

import json
from pathlib import Path

import polars as pl
import pytest

from analysis.bus_prediction_analyzer_utils import (
    AnalyzerConfig,
    IBIBin,
    TimeOfDayBin,
    add_error_columns,
    assign_ibi_bin,
    calculate_ibi_accuracy,
    default_config,
    is_prediction_accurate,
    join_tu_vp,
    load_config,
    narrow_trip_updates,
    narrow_vehicle_positions,
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


def _make_vp_df(
    trip_ids: list[str],
    stop_seqs: list[int],
    statuses: list[str],
    vehicle_ids: list[str],
    timestamps: list[int],
    feed_timestamps: list[int],
) -> pl.DataFrame:
    """Helper to build a vehicle_positions DataFrame with raw GTFS-RT column names."""
    return pl.DataFrame(
        {
            "vehicle.trip.trip_id": trip_ids,
            "vehicle.current_stop_sequence": stop_seqs,
            "vehicle.current_status": statuses,
            "vehicle.vehicle.id": vehicle_ids,
            "vehicle.timestamp": timestamps,
            "feed_timestamp": feed_timestamps,
        }
    )


class TestNarrowVehiclePositions:
    """Tests for narrow_vehicle_positions()."""

    CANONICAL_COLS = {"trip_id", "stop_sequence", "vehicle_id", "actual_timestamp", "vp_feed_timestamp"}

    @pytest.mark.parametrize(
        ["trip_ids", "stop_seqs", "statuses", "vehicle_ids", "timestamps", "feed_ts", "expected_rows"],
        [
            # normal: two STOPPED_AT rows
            (
                ["t1", "t1"],
                [1, 2],
                ["STOPPED_AT", "STOPPED_AT"],
                ["v1", "v1"],
                [1000, 2000],
                [900, 1900],
                2,
            ),
            # filters out IN_TRANSIT_TO
            (
                ["t1", "t1", "t1"],
                [1, 2, 3],
                ["STOPPED_AT", "IN_TRANSIT_TO", "STOPPED_AT"],
                ["v1", "v1", "v1"],
                [1000, 1500, 2000],
                [900, 1400, 1900],
                2,
            ),
            # deduplicates same trip+stop, keeps first
            (
                ["t1", "t1"],
                [1, 1],
                ["STOPPED_AT", "STOPPED_AT"],
                ["v1", "v1"],
                [1000, 1100],
                [900, 1000],
                1,
            ),
            # all filtered out -> 0 rows
            (
                ["t1"],
                [1],
                ["IN_TRANSIT_TO"],
                ["v1"],
                [1000],
                [900],
                0,
            ),
        ],
        ids=["normal", "mixed-statuses", "dedup-same-stop", "all-filtered"],
    )
    def test_row_count(
        self, trip_ids, stop_seqs, statuses, vehicle_ids, timestamps, feed_ts, expected_rows
    ):
        df = _make_vp_df(trip_ids, stop_seqs, statuses, vehicle_ids, timestamps, feed_ts)
        result = narrow_vehicle_positions(df)
        assert result.shape[0] == expected_rows

    def test_output_columns(self):
        df = _make_vp_df(["t1"], [1], ["STOPPED_AT"], ["v1"], [1000], [900])
        result = narrow_vehicle_positions(df)
        assert set(result.columns) == self.CANONICAL_COLS

    def test_sorted_by_trip_and_stop(self):
        df = _make_vp_df(
            ["t2", "t1", "t1"],
            [3, 2, 1],
            ["STOPPED_AT", "STOPPED_AT", "STOPPED_AT"],
            ["v1", "v1", "v1"],
            [3000, 2000, 1000],
            [2900, 1900, 900],
        )
        result = narrow_vehicle_positions(df)
        assert result["trip_id"].to_list() == ["t1", "t1", "t2"]
        assert result["stop_sequence"].to_list() == [1, 2, 3]

    def test_empty_input(self):
        df = _make_vp_df([], [], [], [], [], [])
        result = narrow_vehicle_positions(df)
        assert result.shape[0] == 0
        assert set(result.columns) == self.CANONICAL_COLS


def _make_tu_df(
    trip_ids: list[str],
    stop_seqs: list[int],
    stop_ids: list[str],
    vehicle_ids: list[str],
    arrival_times: list[int | None],
    route_ids: list[str],
    feed_timestamps: list[int],
) -> pl.DataFrame:
    """Helper to build a trip_updates DataFrame with raw GTFS-RT column names."""
    return pl.DataFrame(
        {
            "trip_update.trip.trip_id": trip_ids,
            "trip_update.stop_time_update.stop_sequence": stop_seqs,
            "trip_update.stop_time_update.stop_id": stop_ids,
            "trip_update.vehicle.id": vehicle_ids,
            "trip_update.stop_time_update.arrival.time": arrival_times,
            "trip_update.trip.route_id": route_ids,
            "feed_timestamp": feed_timestamps,
        }
    )


def _make_canonical_vp(
    trip_ids: list[str],
    stop_seqs: list[int],
    vehicle_ids: list[str],
    actual_ts: list[int],
    vp_feed_ts: list[int],
) -> pl.DataFrame:
    """Helper to build an already-narrowed VP DataFrame with canonical column names."""
    return pl.DataFrame(
        {
            "trip_id": trip_ids,
            "stop_sequence": stop_seqs,
            "vehicle_id": vehicle_ids,
            "actual_timestamp": actual_ts,
            "vp_feed_timestamp": vp_feed_ts,
        }
    )


class TestNarrowTripUpdates:
    """Tests for narrow_trip_updates()."""

    CANONICAL_COLS = {"trip_id", "stop_sequence", "stop_id", "vehicle_id", "predicted_arrival", "route_id", "tu_feed_timestamp"}

    def test_output_columns(self):
        df = _make_tu_df(["t1"], [1], ["s1"], ["v1"], [1000], ["r1"], [900])
        result = narrow_trip_updates(df)
        assert set(result.columns) == self.CANONICAL_COLS

    def test_preserves_row_count(self):
        df = _make_tu_df(["t1", "t1"], [1, 2], ["s1", "s2"], ["v1", "v1"], [1000, 2000], ["r1", "r1"], [900, 1900])
        result = narrow_trip_updates(df)
        assert result.shape[0] == 2


class TestJoinTuVp:
    """Tests for join_tu_vp()."""

    @pytest.mark.parametrize(
        ["tu_trips", "tu_seqs", "vp_trips", "vp_seqs", "expected_nulls"],
        [
            # all match
            (["t1", "t1"], [1, 2], ["t1", "t1"], [1, 2], 0),
            # no matches -> actual_timestamp is null
            (["t1"], [1], ["t2"], [1], 1),
            # partial match
            (["t1", "t1"], [1, 2], ["t1"], [1], 1),
        ],
        ids=["all-match", "no-match", "partial-match"],
    )
    def test_join_null_count(self, tu_trips, tu_seqs, vp_trips, vp_seqs, expected_nulls):
        tu = narrow_trip_updates(
            _make_tu_df(tu_trips, tu_seqs, ["s"] * len(tu_trips), ["v1"] * len(tu_trips), [1000] * len(tu_trips), ["r1"] * len(tu_trips), [900] * len(tu_trips))
        )
        vp = _make_canonical_vp(vp_trips, vp_seqs, ["v1"] * len(vp_trips), [1000] * len(vp_trips), [900] * len(vp_trips))
        result = join_tu_vp(tu, vp)
        assert result.shape[0] == len(tu_trips)
        assert result["actual_timestamp"].null_count() == expected_nulls

    def test_preserves_tu_columns(self):
        tu = narrow_trip_updates(_make_tu_df(["t1"], [1], ["s1"], ["v1"], [1000], ["r1"], [900]))
        vp = _make_canonical_vp(["t1"], [1], ["v1"], [999], [890])
        result = join_tu_vp(tu, vp)
        assert "stop_id" in result.columns
        assert "route_id" in result.columns
        assert "actual_timestamp" in result.columns

    def test_empty_tu(self):
        tu = pl.DataFrame(
            schema={
                "trip_id": pl.Utf8,
                "stop_sequence": pl.Int64,
                "stop_id": pl.Utf8,
                "vehicle_id": pl.Utf8,
                "predicted_arrival": pl.Int64,
                "route_id": pl.Utf8,
                "tu_feed_timestamp": pl.Int64,
            }
        )
        vp = _make_canonical_vp(["t1"], [1], ["v1"], [1000], [900])
        result = join_tu_vp(tu, vp)
        assert result.shape[0] == 0


class TestAddErrorColumns:
    """Tests for add_error_columns()."""

    @pytest.mark.parametrize(
        ["predicted", "actual", "feed_ts", "expected_error", "expected_ahead"],
        [
            # predicted late: predicted 1010, actual 1000 -> error = +10
            (1010, 1000, 900, 10, -110),
            # predicted early: predicted 990, actual 1000 -> error = -10
            (990, 1000, 900, -10, -90),
            # exact: no error
            (1000, 1000, 900, 0, -100),
            # prediction made well ahead
            (2000, 2000, 1000, 0, -1000),
        ],
        ids=["predicted-late", "predicted-early", "exact", "far-ahead"],
    )
    def test_error_values(self, predicted, actual, feed_ts, expected_error, expected_ahead):
        df = pl.DataFrame(
            {
                "predicted_arrival": [predicted],
                "actual_timestamp": [actual],
                "tu_feed_timestamp": [feed_ts],
            }
        )
        result = add_error_columns(df)
        assert result["prediction_error_sec"][0] == expected_error
        assert result["prediction_ahead_sec"][0] == expected_ahead

    def test_null_actual_produces_null_error(self):
        df = pl.DataFrame(
            {
                "predicted_arrival": [1000],
                "actual_timestamp": [None],
                "tu_feed_timestamp": [900],
            },
            schema={
                "predicted_arrival": pl.Int64,
                "actual_timestamp": pl.Int64,
                "tu_feed_timestamp": pl.Int64,
            },
        )
        result = add_error_columns(df)
        assert result["prediction_error_sec"][0] is None
        assert result["prediction_ahead_sec"][0] == -100


class TestAssignIbiBin:
    """Tests for assign_ibi_bin()."""

    @pytest.fixture()
    def cfg(self):
        return default_config()

    @pytest.mark.parametrize(
        ["ahead_sec", "expected_bin"],
        [
            # -prediction_ahead_sec maps into bins: [0,180) [180,360) [360,600) [600,900)
            # inside 0-3min bin (90 seconds before arrival)
            (-90, "0-3min"),
            # boundary: exactly 180 -> 3-6min (>= 180)
            (-180, "3-6min"),
            # inside 3-6min
            (-300, "3-6min"),
            # boundary: exactly 360 -> 6-10min
            (-360, "6-10min"),
            # inside 6-10min
            (-500, "6-10min"),
            # boundary: exactly 600 -> 10-15min
            (-600, "10-15min"),
            # inside 10-15min
            (-800, "10-15min"),
            # at 0 -> 0-3min (>= 0)
            (0, "0-3min"),
            # positive ahead_sec = stale prediction (after arrival) -> ignored
            (100, None),
            # outside all bins: >= 900
            (-900, None),
            (-1200, None),
        ],
        ids=[
            "mid-0-3", "boundary-180", "mid-3-6", "boundary-360",
            "mid-6-10", "boundary-600", "mid-10-15", "zero",
            "positive-stale", "boundary-900-outside", "far-outside",
        ],
    )
    def test_bin_assignment(self, cfg, ahead_sec, expected_bin):
        df = pl.DataFrame({"prediction_ahead_sec": [ahead_sec]})
        result = assign_ibi_bin(df, cfg)
        assert result["ibi_bin"][0] == expected_bin

    def test_null_ahead_produces_null_bin(self, cfg):
        df = pl.DataFrame(
            {"prediction_ahead_sec": [None]},
            schema={"prediction_ahead_sec": pl.Int64},
        )
        result = assign_ibi_bin(df, cfg)
        assert result["ibi_bin"][0] is None

    def test_multiple_rows(self, cfg):
        df = pl.DataFrame({"prediction_ahead_sec": [-50, -200, -500, -700, -1000]})
        result = assign_ibi_bin(df, cfg)
        assert result["ibi_bin"].to_list() == ["0-3min", "3-6min", "6-10min", "10-15min", None]

    def test_custom_single_bin(self):
        cfg = AnalyzerConfig(
            ibi_bins=(IBIBin("custom", 0, 60, 10, 20),),
        )
        df = pl.DataFrame({"prediction_ahead_sec": [-30, -61]})
        result = assign_ibi_bin(df, cfg)
        assert result["ibi_bin"].to_list() == ["custom", None]


class TestIsPredictionAccurate:
    """Tests for is_prediction_accurate()."""

    @pytest.fixture()
    def cfg(self):
        return default_config()

    @pytest.mark.parametrize(
        ["ibi_bin", "error_sec", "expected"],
        [
            # 0-3min: early_threshold=30, late_threshold=90
            ("0-3min", 0, True),
            ("0-3min", 90, True),       # exactly on late boundary (inclusive)
            ("0-3min", 91, False),      # just over late
            ("0-3min", -30, True),      # exactly on early boundary (inclusive)
            ("0-3min", -31, False),     # just over early
            # 3-6min: early=60, late=150
            ("3-6min", 150, True),
            ("3-6min", 151, False),
            ("3-6min", -60, True),
            ("3-6min", -61, False),
            # null bin -> null accuracy
            (None, 50, None),
        ],
        ids=[
            "0-3-exact", "0-3-late-boundary", "0-3-over-late",
            "0-3-early-boundary", "0-3-over-early",
            "3-6-late-boundary", "3-6-over-late",
            "3-6-early-boundary", "3-6-over-early",
            "null-bin",
        ],
    )
    def test_accuracy_flag(self, cfg, ibi_bin, error_sec, expected):
        df = pl.DataFrame(
            {"ibi_bin": [ibi_bin], "prediction_error_sec": [error_sec]},
            schema={"ibi_bin": pl.Utf8, "prediction_error_sec": pl.Int64},
        )
        result = is_prediction_accurate(df, cfg)
        assert result["is_accurate"][0] == expected


class TestCalculateIbiAccuracy:
    """Tests for calculate_ibi_accuracy()."""

    @pytest.fixture()
    def cfg(self):
        return default_config()

    def test_all_accurate(self, cfg):
        df = pl.DataFrame(
            {"ibi_bin": ["0-3min", "0-3min", "3-6min"], "is_accurate": [True, True, True]}
        )
        result = calculate_ibi_accuracy(df, cfg)
        overall = result.filter(pl.col("ibi_bin") == "overall")
        assert overall["accuracy_pct"][0] == 100.0

    def test_none_accurate(self, cfg):
        df = pl.DataFrame(
            {"ibi_bin": ["0-3min", "3-6min"], "is_accurate": [False, False]}
        )
        result = calculate_ibi_accuracy(df, cfg)
        overall = result.filter(pl.col("ibi_bin") == "overall")
        assert overall["accuracy_pct"][0] == 0.0

    def test_mixed_accuracy(self, cfg):
        # 0-3min: 1/2 = 50%, 3-6min: 1/1 = 100%, overall = (50+100)/2 = 75
        df = pl.DataFrame(
            {"ibi_bin": ["0-3min", "0-3min", "3-6min"], "is_accurate": [True, False, True]}
        )
        result = calculate_ibi_accuracy(df, cfg)
        overall = result.filter(pl.col("ibi_bin") == "overall")
        assert overall["accuracy_pct"][0] == 75.0

    def test_empty_after_filter(self, cfg):
        df = pl.DataFrame(
            {"ibi_bin": [None], "is_accurate": [None]},
            schema={"ibi_bin": pl.Utf8, "is_accurate": pl.Boolean},
        )
        result = calculate_ibi_accuracy(df, cfg)
        assert result.shape[0] == 0

    def test_per_bin_counts(self, cfg):
        df = pl.DataFrame(
            {"ibi_bin": ["0-3min", "0-3min", "0-3min"], "is_accurate": [True, True, False]}
        )
        result = calculate_ibi_accuracy(df, cfg)
        row = result.filter(pl.col("ibi_bin") == "0-3min")
        assert row["total"][0] == 3
        assert row["accurate"][0] == 2
