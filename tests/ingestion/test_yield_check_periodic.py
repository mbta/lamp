"""Tests for time-based periodic yielding in GtfsRtConverter."""

from datetime import datetime
from queue import Queue
from typing import List, Tuple

import pyarrow
import pytest

from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import group_sort_file_list
from lamp_py.runtime_utils.process_logger import ProcessLogger


def make_converter(time_chunk_minutes: int = 15) -> GtfsRtConverter:
    """Create a converter with periodic yielding enabled."""
    return GtfsRtConverter(
        config_type=ConfigType.RT_ALERTS,
        metadata_queue=Queue(),
        time_chunk_minutes=time_chunk_minutes,
    )


def make_dummy_table(num_rows: int = 10) -> pyarrow.Table:
    """Create a minimal pyarrow table for testing."""
    return pyarrow.table({"col": list(range(num_rows))})


@pytest.mark.parametrize(
    "chunk_minutes, input_ts, expected_ts",
    [
        # 15-minute chunks
        pytest.param(15, datetime(2026, 5, 4, 1, 15, 0), datetime(2026, 5, 4, 1, 15), id="15m-on-boundary"),
        pytest.param(15, datetime(2026, 5, 4, 1, 14, 59), datetime(2026, 5, 4, 1, 0), id="15m-just-before-boundary"),
        pytest.param(15, datetime(2026, 5, 4, 1, 15, 1), datetime(2026, 5, 4, 1, 15), id="15m-just-after-boundary"),
        pytest.param(15, datetime(2026, 5, 4, 1, 0, 0), datetime(2026, 5, 4, 1, 0), id="15m-start-of-hour"),
        pytest.param(15, datetime(2026, 5, 4, 23, 59, 59), datetime(2026, 5, 4, 23, 45), id="15m-end-of-day"),
        pytest.param(15, datetime(2026, 5, 4, 0, 0, 0), datetime(2026, 5, 4, 0, 0), id="15m-midnight"),
        # 30-minute chunks
        pytest.param(30, datetime(2026, 5, 4, 1, 29), datetime(2026, 5, 4, 1, 0), id="30m-before-boundary"),
        pytest.param(30, datetime(2026, 5, 4, 1, 30), datetime(2026, 5, 4, 1, 30), id="30m-on-boundary"),
        pytest.param(30, datetime(2026, 5, 4, 1, 59), datetime(2026, 5, 4, 1, 30), id="30m-end-of-hour"),
        # 5-minute chunks
        pytest.param(5, datetime(2026, 5, 4, 1, 7), datetime(2026, 5, 4, 1, 5), id="5m-mid-interval"),
        pytest.param(5, datetime(2026, 5, 4, 1, 10), datetime(2026, 5, 4, 1, 10), id="5m-on-boundary"),
        # 60-minute chunks
        pytest.param(60, datetime(2026, 5, 4, 1, 59), datetime(2026, 5, 4, 1, 0), id="60m-end-of-hour"),
        pytest.param(60, datetime(2026, 5, 4, 2, 0), datetime(2026, 5, 4, 2, 0), id="60m-on-boundary"),
    ],
)
def test_interval_key(chunk_minutes: int, input_ts: datetime, expected_ts: datetime) -> None:
    """_interval_key truncates timestamps to wall-clock-aligned interval starts."""
    c = make_converter(chunk_minutes)
    assert c._interval_key(input_ts) == expected_ts


@pytest.mark.parametrize(
    "interval_minutes, current_ts, flush, expected_yield_count, expected_remaining_keys",
    [
        # current_ts in same interval as data — should NOT yield
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 20),
            False,
            0,
            [datetime(2026, 5, 4, 1, 15)],
            id="same-interval-no-yield",
        ),
        # current_ts before interval start — should yield (reverse order)
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 10),
            False,
            1,
            [],
            id="past-interval-yields",
        ),
        # current_ts at end of interval — should NOT yield
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 29),
            False,
            0,
            [datetime(2026, 5, 4, 1, 15)],
            id="end-of-interval-no-yield",
        ),
        # flush yields everything regardless of current_ts
        pytest.param(
            [(1, 0), (1, 15)],
            datetime(2026, 5, 4, 23, 59),
            True,
            2,
            [],
            id="flush-yields-all",
        ),
        # selective yield: current at 01:00, should yield 01:15 and 01:30 but not 01:00
        pytest.param(
            [(1, 0), (1, 15), (1, 30)],
            datetime(2026, 5, 4, 1, 10),
            False,
            2,
            [datetime(2026, 5, 4, 1, 0)],
            id="selective-yield-keeps-current",
        ),
    ],
)
def test_yield_check_periodic(
    interval_minutes: List[Tuple[int, int]],
    current_ts: datetime,
    flush: bool,
    expected_yield_count: int,
    expected_remaining_keys: List[datetime],
) -> None:
    """yield_check_periodic yields completed intervals based on current_ts position."""
    c = make_converter(15)
    logger = ProcessLogger("test")
    logger.log_start()

    for hour, minute in interval_minutes:
        key = datetime(2026, 5, 4, hour, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1)
        c.data_parts[key].files = [f"file_{hour}_{minute}.json.gz"]

    tables = list(c.yield_check_periodic(logger, current_ts, flush=flush))

    assert len(tables) == expected_yield_count
    assert list(c.data_parts.keys()) == expected_remaining_keys


def test_yield_check_periodic_skips_none_table() -> None:
    """Intervals with None table should not be yielded even when flush=True."""
    c = make_converter(15)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 15)
    c.data_parts[key] = TableData()  # table is None by default

    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 0, 50), flush=True))
    assert len(tables) == 0


def test_yield_check_periodic_archives_files() -> None:
    """Yielded intervals should move their files to archive_files."""
    c = make_converter(15)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 15)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(5)
    c.data_parts[key].files = ["a.json.gz", "b.json.gz"]

    list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 0)))

    assert "a.json.gz" in c.archive_files
    assert "b.json.gz" in c.archive_files


@pytest.mark.parametrize(
    "reverse, expected_first_ts, expected_last_ts",
    [
        pytest.param(True, "01:30:00Z", "01:00:00Z", id="reverse-newest-first"),
        pytest.param(False, "01:00:00Z", "01:30:00Z", id="forward-oldest-first"),
    ],
)
def test_group_sort_file_sort_order(reverse: bool, expected_first_ts: str, expected_last_ts: str) -> None:
    """group_sort_file_list sorts files chronologically with optional reverse."""
    files = [
        "s3://bucket/lamp/delta/2026/05/04/2026-05-04T01:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz",
        "s3://bucket/lamp/delta/2026/05/04/2026-05-04T01:15:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz",
        "s3://bucket/lamp/delta/2026/05/04/2026-05-04T01:30:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz",
    ]

    result = group_sort_file_list(files, reverse=reverse)
    group = result["https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"]
    assert expected_first_ts in group[0]
    assert expected_last_ts in group[-1]
