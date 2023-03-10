import logging
import os
from functools import lru_cache
from typing import Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pyarrow
import pytest
import sqlalchemy as sa
from _pytest.monkeypatch import MonkeyPatch
from pyarrow import fs, parquet

from lamp_py.performance_manager import process_static_tables

from lamp_py.performance_manager.l0_gtfs_rt_events import (
    combine_events,
    get_gtfs_rt_paths,
    process_gtfs_rt_files,
    upload_to_database,
)
from lamp_py.performance_manager.l0_rt_trip_updates import (
    get_and_unwrap_tu_dataframe,
    join_tu_with_gtfs_static,
    reduce_trip_updates,
)
from lamp_py.performance_manager.l0_rt_vehicle_positions import (
    get_vp_dataframe,
    join_vp_with_gtfs_static,
    transform_vp_datatypes,
    transform_vp_timestamps,
)
from lamp_py.postgres.postgres_schema import (
    MetadataLog,
    StaticCalendar,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    VehicleEvents,
)
from lamp_py.postgres.postgres_utils import DatabaseManager

HERE = os.path.dirname(os.path.abspath(__file__))
LAMP_DIR = os.path.join(HERE, "test_files", "SPRINGBOARD")


@lru_cache
def test_files() -> List[str]:
    """
    collaps all of the files in the lamp test files dir into a list
    """
    paths = []

    for root, _, files in os.walk(LAMP_DIR):
        for file in files:
            if "DS_Store" in file:
                continue
            paths.append(os.path.join(root, file))

    paths.sort()
    return paths


def set_env_vars() -> None:
    """
    boostrap .env file for local testing
    """
    if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
        logging.warning("allready bootstrapped")
    else:
        env_file = os.path.join(HERE, "..", "..", ".env")
        logging.debug("bootstrapping with env file %s", env_file)
        print("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.debug("setting %s to %s", key, value)
                os.environ[key] = value


@pytest.fixture(scope="module", name="db_manager")
def fixture_db_manager() -> DatabaseManager:
    """
    generate a database manager for all of our tests
    """
    set_env_vars()
    db_manager = DatabaseManager()
    return db_manager


@pytest.fixture(autouse=True, name="s3_patch")
def fixture_s3_patch(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """
    insert a monkeypatch over s3 related functions. our tests use local
    files instead of s3, so we read these files differently
    """

    def mock_get_static_parquet_paths(
        table_type: str, feed_info_path: str
    ) -> List[str]:
        """
        instead of mocking up s3 responses, just rewrite this method and
        monkeypatch it
        """
        logging.debug(
            "Mock Static Parquet Paths table_type=%s, feed_info_path=%s",
            table_type,
            feed_info_path,
        )
        table_dir = feed_info_path.replace("FEED_INFO", table_type)
        files = [os.path.join(table_dir, f) for f in os.listdir(table_dir)]
        logging.debug("Mock Static Parquet Paths return%s", files)
        return files

    monkeypatch.setattr(
        "lamp_py.performance_manager.l0_gtfs_static_table.get_static_parquet_paths",
        mock_get_static_parquet_paths,
    )

    def mock__get_pyarrow_table(
        filename: Union[str, List[str]],
        filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
    ) -> pyarrow.Table:
        logging.debug(
            "Mock Get Pyarrow Table filename=%s, filters=%s", filename, filters
        )
        active_fs = fs.LocalFileSystem()

        if isinstance(filename, list):
            to_load = filename
        else:
            to_load = [filename]

        if len(to_load) == 0:
            return pyarrow.Table.from_pydict({})

        return parquet.ParquetDataset(
            to_load, filesystem=active_fs, filters=filters
        ).read_pandas()

    monkeypatch.setattr(
        "lamp_py.aws.s3._get_pyarrow_table", mock__get_pyarrow_table
    )
    yield


def check_logs(caplog: pytest.LogCaptureFixture) -> None:
    """
    utility to check that all logged statements were properly formatted, no
    errors were reported, and that all processes had a log start and log complete.
    """

    def message_to_dict(message: str) -> Dict[str, str]:
        """convert our log messages into dict of properties"""
        message_dict = {}

        for part in message.replace(",", "").split(" "):
            try:
                (key, value) = part.split("=", 1)
                message_dict[key] = value
            except Exception as exception:
                pytest.fail(
                    f"Unable to parse log message {message}. Reason {exception}"
                )
        return message_dict

    # keep track of logged processes to ensure order is correct
    process_stack: List[Dict[str, str]] = []

    for record in caplog.records:
        # if the record was an Error Level, fail immediately
        if record.levelname == "ERROR":
            pytest.fail(f"Error messages encountered {record.message}")

        # skip debug level logs
        if record.levelname == "DEBUG":
            continue

        log = message_to_dict(record.message)

        # check for keys that must be present
        must_keys = ["status", "uuid", "parent", "process_id", "process_name"]
        for key in must_keys:
            if key not in log:
                pytest.fail(f"Log missing {key} key - {record.message}")

        # process the record, ensuring proper ordering
        if log["status"] == "complete":
            # process should be at the end of the stack
            if process_stack[-1]["uuid"] != log["uuid"]:
                pytest.fail(
                    f"Improper Ordering of Log Statements {caplog.text}"
                )
            if "duration" not in log:
                pytest.fail(f"Log missing duration key - {record.message}")
            process_stack.pop()
        elif log["status"] == "started":
            process_stack.append(log)

    if len(process_stack) != 0:
        pytest.fail(f"Not all processes logged completion {caplog.text}")


def test_static_tables(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test that static schedule files are loaded correctly into our db
    """
    caplog.set_level(logging.INFO)

    paths = [file for file in test_files() if "FEED_INFO" in file]
    db_manager.add_metadata_paths(paths)

    unprocessed_static_schedules = db_manager.select_as_list(
        sa.select(MetadataLog.path).where(
            (MetadataLog.processed == sa.false())
            & (MetadataLog.path.contains("FEED_INFO"))
        )
    )

    process_static_tables(db_manager)

    # these are the row counts in the parquet files computed in a jupyter
    # notebook without using any of our module. our module should be taking
    # these parquet files and dropping them into our db directly. after
    # processing the static tables our db tables should have these many record
    # counts.
    row_counts = {
        StaticTrips: 64879,
        StaticRoutes: 234,
        StaticStops: 9706,
        StaticStopTimes: 1676197,
        StaticCalendar: 76,
    }

    with db_manager.session.begin() as session:
        for table, should_count in row_counts.items():
            actual_count = session.query(table).count()
            tablename = table.__tablename__  # type: ignore
            assert (
                actual_count == should_count
            ), f"Table {tablename} has incorrect row count"

    unprocessed_static_schedules = db_manager.select_as_list(
        sa.select(MetadataLog.path).where(
            (MetadataLog.processed == sa.false())
            & (MetadataLog.path.contains("FEED_INFO"))
        )
    )

    assert len(unprocessed_static_schedules) == 0

    check_logs(caplog)


def test_gtfs_rt_processing(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test that vehicle position and trip updates files can be consumed properly
    """
    caplog.set_level(logging.INFO)
    db_manager.truncate_table(VehicleEvents)

    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        file
        for file in test_files()
        if ("RT_VEHICLE_POSITIONS" in file or "RT_TRIP_UPDATES" in file)
        and ("hour=10" in file or "hour=11" in file)
    ]
    db_manager.add_metadata_paths(paths)

    grouped_files = get_gtfs_rt_paths(db_manager)

    for files in grouped_files:
        for path in files["vp_paths"]:
            assert "RT_VEHICLE_POSITIONS" in path

        # check that we can load the parquet file into a dataframe correctly
        positions = get_vp_dataframe(files["vp_paths"])
        position_size = positions.shape[0]
        assert positions.shape[1] == 9

        # check that the types can be set correctly
        positions = transform_vp_datatypes(positions)
        assert positions.shape[1] == 9
        assert position_size == positions.shape[0]

        # check that it can be combined with the static schedule
        positions = join_vp_with_gtfs_static(positions, db_manager)
        assert positions.shape[1] == 11
        assert position_size == positions.shape[0]

        positions = transform_vp_timestamps(positions)
        assert positions.shape[1] == 12
        assert positions.shape[0] < position_size

        trip_updates = get_and_unwrap_tu_dataframe(files["tu_paths"])
        trip_update_size = trip_updates.shape[0]
        assert trip_updates.shape[1] == 9

        trip_updates = join_tu_with_gtfs_static(trip_updates, db_manager)
        assert trip_updates.shape[1] == 11
        assert trip_update_size == trip_updates.shape[0]

        trip_updates = reduce_trip_updates(trip_updates)
        assert trip_update_size > trip_updates.shape[0]

        events = combine_events(positions, trip_updates)

        ve_columns = [c.key for c in VehicleEvents.__table__.columns]
        # pk id and updated on are handled by postgres. trip hash is added just
        # before inserting new rows to the db.
        expected_columns = set(ve_columns) - {
            "pk_id",
            "updated_on",
            "trip_hash",
        }
        assert len(expected_columns) == len(events.columns)

        missing_columns = set(events.columns) - expected_columns
        assert len(missing_columns) == 0

        upload_to_database(events, db_manager)

    check_logs(caplog)


def test_vp_only(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test the vehicle positions can be updated without trip updates
    """
    caplog.set_level(logging.INFO)

    db_manager.truncate_table(VehicleEvents)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        p
        for p in test_files()
        if "RT_VEHICLE_POSITIONS" in p and ("hourt=10" in p or "hour=11" in p)
    ]
    db_manager.add_metadata_paths(paths)

    process_gtfs_rt_files(db_manager)

    check_logs(caplog)


def test_tu_only(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test the trip updates can be processed without vehicle positions
    """
    caplog.set_level(logging.INFO)

    db_manager.truncate_table(VehicleEvents)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        p
        for p in test_files()
        if "RT_TRIP_UPDATES" in p and ("hourt=10" in p or "hour=11" in p)
    ]

    db_manager.add_metadata_paths(paths)

    process_gtfs_rt_files(db_manager)

    check_logs(caplog)


def test_vp_and_tu(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test that vehicle positions and trip updates can be processed together
    """
    caplog.set_level(logging.INFO)

    db_manager.truncate_table(VehicleEvents)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [p for p in test_files() if "hourt=10" in p or "hour=11" in p]
    db_manager.add_metadata_paths(paths)

    process_gtfs_rt_files(db_manager)

    check_logs(caplog)
