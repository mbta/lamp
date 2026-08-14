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
    time_chunk_minutes: int = 15,
    move_source_on_completion: bool = False,
    unique_config: bool = True,
    lookback_count: int = 3,
) -> GtfsRtFullPartitionConverter:
    """Create a converter with periodic yielding enabled."""
    return GtfsRtFullPartitionConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        move_source_on_completion=move_source_on_completion,
        time_chunk_minutes=time_chunk_minutes,
        unique_config=unique_config,
        lookback_count=lookback_count,
    )


def make_dummy_table(num_rows: int = 10, feed_timestamp: int = 0) -> pyarrow.Table:
    """Create a minimal pyarrow table for testing."""
    return pyarrow.table({"col": list(range(num_rows)), "feed_timestamp": [feed_timestamp] * num_rows})


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
    "chunk_minutes, anchor, lookback, expected_keys",
    [
        pytest.param(
            15,
            datetime(2026, 5, 4, 1, 30),
            0,
            [datetime(2026, 5, 4, 1, 30)],
            id="15m-no-lookback",
        ),
        pytest.param(
            15,
            datetime(2026, 5, 4, 1, 30),
            2,
            [datetime(2026, 5, 4, 1, 30), datetime(2026, 5, 4, 1, 15), datetime(2026, 5, 4, 1, 0)],
            id="15m-lookback-2",
        ),
        pytest.param(
            15,
            datetime(2026, 5, 4, 1, 30),
            3,
            [
                datetime(2026, 5, 4, 1, 30),
                datetime(2026, 5, 4, 1, 15),
                datetime(2026, 5, 4, 1, 0),
                datetime(2026, 5, 4, 0, 45),
            ],
            id="15m-lookback-3",
        ),
        pytest.param(
            30,
            datetime(2026, 5, 4, 2, 0),
            1,
            [datetime(2026, 5, 4, 2, 0), datetime(2026, 5, 4, 1, 30)],
            id="30m-lookback-1",
        ),
        pytest.param(
            15,
            datetime(2026, 5, 4, 0, 0),
            1,
            [datetime(2026, 5, 4, 0, 0), datetime(2026, 5, 3, 23, 45)],
            id="15m-crosses-midnight",
        ),
    ],
)
def test_interval_keys(
    chunk_minutes: int,
    anchor: datetime,
    lookback: int,
    expected_keys: List[datetime],
) -> None:
    """interval_keys returns the anchor key plus lookback_count prior keys."""
    c = make_converter(time_chunk_minutes=chunk_minutes)
    assert c.interval_keys(anchor, lookback) == expected_keys


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
            [datetime(2026, 5, 4, 1, 15)],
            id="later-interval-yields-keeps-for-lookback",
        ),
        # flush yields everything regardless of current_ts
        # with unique=(True, 3), yielded chunks are retained for lookback
        pytest.param(
            [(1, 0), (1, 15)],
            datetime(2026, 5, 4, 23, 59),
            True,
            2,
            [datetime(2026, 5, 4, 1, 0), datetime(2026, 5, 4, 1, 15)],
            id="flush-yields-all",
        ),
        # selective yield: current at 01:20, should yield 01:00 but not 01:15 or 01:30
        pytest.param(
            [(1, 0), (1, 15), (1, 30)],
            datetime(2026, 5, 4, 1, 20),
            False,
            1,
            [datetime(2026, 5, 4, 1, 0), datetime(2026, 5, 4, 1, 15), datetime(2026, 5, 4, 1, 30)],
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
    c = make_converter(15, unique_config=True, lookback_count=3)
    logger = ProcessLogger("test")
    logger.log_start()

    for hour, minute in interval_minutes:
        key = datetime(2026, 5, 4, hour, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"file_{hour}_{minute}.json.gz"]

    tables = list(c.yield_check_periodic(logger, current_ts, flush=flush))

    assert len(tables) == expected_yield_count
    assert sorted(c.data_parts.keys()) == sorted(expected_remaining_keys)


@pytest.mark.parametrize("move", [True, False])
def test_yield_check_periodic_archives_files(move: bool) -> None:
    """Yielded intervals should move their files to archive_files."""
    c = make_converter(15, move_source_on_completion=move, unique_config=False, lookback_count=0)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 15)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(5, feed_timestamp=int(key.timestamp()))
    c.data_parts[key].files = ["a.json.gz", "b.json.gz"]

    list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 40)))

    assert ("a.json.gz" in c.archive_files) == move
    assert ("b.json.gz" in c.archive_files) == move


def test_clean_local_folders_removes_oldest_day(tmp_path: Path) -> None:
    """
    clean_local_folders should keep only the two newest day partitions
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


def test_no_unique_deletes_immediately() -> None:
    """With unique disabled, yielded chunks are deleted from data_parts right away."""
    c = make_converter(15, unique_config=False, lookback_count=0)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 0)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(1)
    c.data_parts[key].files = ["f.json.gz"]

    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 20)))
    assert len(tables) == 1
    assert key not in c.data_parts


def test_unique_retains_within_lookback() -> None:
    """With unique enabled, yielded chunks stay in data_parts until they fall out of the lookback window."""
    c = make_converter(15, unique_config=True, lookback_count=2)
    logger = ProcessLogger("test")
    logger.log_start()

    # populate 4 consecutive 15-min intervals: 01:00, 01:15, 01:30, 01:45
    for minute in [0, 15, 30, 45]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    # current_ts at 02:05 -> current_interval = 02:00
    # all four intervals are older than 02:00, so all should be yielded
    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 2, 5)))
    assert len(tables) == 4

    # with lookback=2, the last yielded chunk is 01:45
    # oldest_keep for 01:45 is 01:45 - 2*15min = 01:15
    # so 01:00 should be evicted, but 01:15, 01:30, 01:45 retained
    assert datetime(2026, 5, 4, 1, 0) not in c.data_parts
    assert datetime(2026, 5, 4, 1, 15) in c.data_parts
    assert datetime(2026, 5, 4, 1, 30) in c.data_parts
    assert datetime(2026, 5, 4, 1, 45) in c.data_parts


def test_unique_evicts_oldest_chunks_progressively() -> None:
    """As new intervals are yielded, older chunks beyond lookback are evicted."""
    c = make_converter(15, unique_config=True, lookback_count=1)
    logger = ProcessLogger("test")
    logger.log_start()

    # simulate processing 01:00, 01:15, 01:30 sequentially
    for minute in [0, 15, 30]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    # current at 01:50 -> yields 01:00, 01:15, 01:30
    # lookback=1 means each yielded chunk keeps itself + 1 prior
    # last yielded is 01:30, oldest_keep = 01:30 - 15 = 01:15
    # so 01:00 is evicted
    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 50)))
    assert len(tables) == 3
    assert datetime(2026, 5, 4, 1, 0) not in c.data_parts
    assert datetime(2026, 5, 4, 1, 15) in c.data_parts
    assert datetime(2026, 5, 4, 1, 30) in c.data_parts


def test_flush_with_unique_evicts_old_chunks() -> None:
    """Flush with unique enabled still evicts chunks beyond the lookback window."""
    c = make_converter(15, unique_config=True, lookback_count=1)
    logger = ProcessLogger("test")
    logger.log_start()

    for minute in [0, 15, 30]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    tables = list(c.yield_check_periodic(logger, flush=True))
    assert len(tables) == 3
    # last yielded is 01:30, lookback=1, oldest_keep = 01:15
    assert datetime(2026, 5, 4, 1, 0) not in c.data_parts
    assert datetime(2026, 5, 4, 1, 15) in c.data_parts
    assert datetime(2026, 5, 4, 1, 30) in c.data_parts


def test_flush_without_unique_deletes_all() -> None:
    """Flush with unique disabled deletes all yielded chunks immediately."""
    c = make_converter(15, unique_config=False, lookback_count=0)
    logger = ProcessLogger("test")
    logger.log_start()

    for minute in [0, 15, 30]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1)
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    tables = list(c.yield_check_periodic(logger, flush=True))
    assert len(tables) == 3
    assert len(c.data_parts) == 0


def test_no_unique_archives_on_delete() -> None:
    """With unique disabled and move_source_on_completion, files are archived when chunk is deleted."""
    c = make_converter(15, move_source_on_completion=True, unique_config=False, lookback_count=0)
    logger = ProcessLogger("test")
    logger.log_start()

    for minute in [0, 15]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1)
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 35)))
    assert "f_0.json.gz" in c.archive_files
    assert "f_15.json.gz" in c.archive_files
    assert len(c.data_parts) == 0


def test_lookback_zero_with_unique_deletes_immediately() -> None:
    """unique_config=True but lookback_count=0 behaves like no-unique (no dedup possible)."""
    c = make_converter(15, unique_config=True, lookback_count=0)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 0)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
    c.data_parts[key].files = ["f.json.gz"]

    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 20)))
    assert len(tables) == 1
    assert key not in c.data_parts


def test_yield_only_once_across_calls() -> None:
    """A chunk is yielded only once even if yield_check_periodic is called multiple times."""
    c = make_converter(15, unique_config=True, lookback_count=2)
    logger = ProcessLogger("test")
    logger.log_start()

    key = datetime(2026, 5, 4, 1, 0)
    c.data_parts[key] = TableData()
    c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
    c.data_parts[key].files = ["f.json.gz"]

    # first call yields the chunk
    tables1 = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 20)))
    assert len(tables1) == 1

    # second call should NOT yield it again
    tables2 = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 35)))
    assert len(tables2) == 0

    # chunk still retained for lookback
    assert key in c.data_parts
    assert c.data_parts[key].yielded is True


def test_eviction_archives_files_for_unique_mode() -> None:
    """In unique mode, files are archived when a chunk is evicted (not when yielded)."""
    c = make_converter(15, move_source_on_completion=True, unique_config=True, lookback_count=1)
    logger = ProcessLogger("test")
    logger.log_start()

    for minute in [0, 15, 30]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    # yields all three; lookback=1, last yielded is 01:30, oldest_keep=01:15
    # 01:00 evicted -> its files go to archive
    list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 50)))

    assert "f_0.json.gz" in c.archive_files
    # 01:15 and 01:30 are still retained, their files are NOT yet archived
    assert "f_15.json.gz" not in c.archive_files
    assert "f_30.json.gz" not in c.archive_files


def test_data_parts_size_two_yields_older() -> None:
    """With exactly 2 data_parts, the older one is yielded when current moves past it."""
    c = make_converter(15, unique_config=True, lookback_count=1)
    logger = ProcessLogger("test")
    logger.log_start()

    for minute in [0, 15]:
        key = datetime(2026, 5, 4, 1, minute)
        c.data_parts[key] = TableData()
        c.data_parts[key].table = make_dummy_table(1, feed_timestamp=int(key.timestamp()))
        c.data_parts[key].files = [f"f_{minute}.json.gz"]

    # current at 01:20 -> interval 01:15; 01:15 > 01:00 so yields 01:00
    tables = list(c.yield_check_periodic(logger, datetime(2026, 5, 4, 1, 20)))
    assert len(tables) == 1
    assert tables[0][1] == datetime(2026, 5, 4, 1, 0)

    # lookback=1 for 01:00: keys=[01:00, 00:45], oldest_keep=00:45, nothing to evict
    assert datetime(2026, 5, 4, 1, 0) in c.data_parts
    assert datetime(2026, 5, 4, 1, 15) in c.data_parts
