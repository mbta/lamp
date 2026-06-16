from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import List, Tuple

import pyarrow
import pytest

from lamp_py.ingestion.convert_gtfs_rt import TableData
from lamp_py.ingestion.convert_gtfs_rt_fullset import GtfsRtFullPartitionConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import assign_datetime_to_binned_interval
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP


def make_converter(
    time_chunk_minutes: int = 15, move_source_on_completion: bool = False
) -> GtfsRtFullPartitionConverter:
    """Create a converter with periodic yielding enabled."""
    return GtfsRtFullPartitionConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        move_source_on_completion=move_source_on_completion,
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
def test_assign_datetime_to_binned_interval(chunk_minutes: int, input_ts: datetime, expected_ts: datetime) -> None:
    """assign_datetime_to_binned_interval truncates timestamps to wall-clock-aligned interval starts."""
    assert assign_datetime_to_binned_interval(input_ts, chunk_minutes) == expected_ts


@pytest.mark.parametrize(
    "interval_minutes, current_ts, flush, expected_yield_count, expected_remaining_keys",
    [
        # current_ts in same interval as data: should not yield
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 20),
            False,
            0,
            [datetime(2026, 5, 4, 1, 15)],
            id="same-interval-no-yield",
        ),
        # current_ts before interval start: should not yield
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 10),
            False,
            0,
            [datetime(2026, 5, 4, 1, 15)],
            id="past-interval-no-yield",
        ),
        # current_ts in a later interval: should yield old interval
        pytest.param(
            [(1, 15)],
            datetime(2026, 5, 4, 1, 35),
            False,
            1,
            [],
            id="later-interval-yields",
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
            datetime(2026, 5, 4, 1, 20),
            False,
            1,
            [datetime(2026, 5, 4, 1, 15), datetime(2026, 5, 4, 1, 30)],
            id="selective-yield-older-only",
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


@pytest.mark.parametrize("flush", [True, False])
def test_yield_check_periodic_skips_none_table(flush: bool) -> None:
    """Intervals with None table should not be yielded even when flush=True."""
    c = make_converter(15)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 15)
    c.data_parts[key] = TableData()  # table is None by default

    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 0, 50), flush=flush))
    assert len(tables) == 0 if flush else 1


@pytest.mark.parametrize("move", [True, False])
def test_yield_check_periodic_archives_files(move: bool) -> None:
    """Yielded intervals should move their files to archive_files."""
    c = make_converter(15, move_source_on_completion=move)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 15)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(5)
    c.data_parts[key].files = ["a.json.gz", "b.json.gz"]

    list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 40)))

    assert ("a.json.gz" in c.archive_files) == move
    assert ("b.json.gz" in c.archive_files) == move


def test_clean_local_folders_removes_oldest_day(tmp_path: Path) -> None:
    """
    clean_local_folders should keep only the two newest day partitions.
    Handles case when ingestion is down for a duration exceeding a couple days
    """
    c = GtfsRtFullPartitionConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        local_output_location=tmp_path.as_posix(),
    )

    root = tmp_path / LAMP / str(ConfigType.RT_TRIP_UPDATES)
    day_folders = [
        root / "year=2026" / "month=5" / "day=1",
        root / "year=2026" / "month=5" / "day=2",
        root / "year=2026" / "month=5" / "day=3",
    ]
    for day_folder in day_folders:
        day_folder.mkdir(parents=True, exist_ok=True)
        (day_folder / "part.parquet").write_text("x", encoding="utf-8")

    c.clean_local_folders()

    assert not day_folders[0].exists()
    assert day_folders[1].exists()
    assert day_folders[2].exists()
