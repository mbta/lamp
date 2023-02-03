import logging
import os
from typing import Iterator, List

import pandas
import pytest
import sqlalchemy as sa
from _pytest.monkeypatch import MonkeyPatch
from lib import process_static_tables
from lib.l0_gtfs_rt_events import (
    collapse_events,
    get_gtfs_rt_paths,
    upload_to_database,
)
from lib.l0_rt_trip_updates import (
    get_and_unwrap_tu_dataframe,
    join_tu_with_gtfs_static,
    reduce_trip_updates,
)
from lib.l0_rt_vehicle_positions import (
    get_vp_dataframe,
    join_vp_with_gtfs_static,
    transform_vp_dtypes,
    transform_vp_timestamps,
)
from lib.postgres_schema import (
    MetadataLog,
    StaticCalendar,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    VehicleEvents,
)
from lib.postgres_utils import DatabaseManager

HERE = os.path.dirname(os.path.abspath(__file__))
LAMP_DIR = os.path.join(HERE, "test_files", "lamp")


def set_env_vars() -> None:
    """
    boostrap .env file for local testing
    """
    if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
        logging.warning("allready bootstrapped")
    else:
        env_file = os.path.join(HERE, "..", "..", ".env")
        logging.debug("bootstrapping with env file %s", env_file)

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


@pytest.fixture(autouse=True, scope="module", name="add_files")
def fixture_add_files(db_manager: DatabaseManager) -> Iterator[None]:
    """
    seed the files in our test files filesystem into the metadata tablPae
    """
    metadata_rows = 0
    with db_manager.session.begin() as session:
        metadata_rows = session.query(MetadataLog).count()

    if metadata_rows != 0:
        yield
    else:

        paths = []

        for root, _, files in os.walk(LAMP_DIR):
            for file in files:
                if "DS_Store" in file:
                    continue
                path = os.path.join(root, file)
                if "FEED_INFO" in path:
                    paths.append(path)
                elif (
                    "RT_VEHICLE_POSITIONS" in path or "RT_TRIP_UPDATES" in path
                ):
                    if "hour=10" in path or "hour=11" in path:
                        paths.append(path)

        logging.info("Seeding %s paths", len(paths))

        paths.sort()

        with db_manager.session.begin() as session:
            session.execute(
                sa.insert(MetadataLog.__table__), [{"path": p} for p in paths]
            )

        yield


def test_static_tables(
    db_manager: DatabaseManager, monkeypatch: MonkeyPatch
) -> None:
    """
    test that static schedule files are loaded correctly into our db
    """

    def mock_get_static_parquet_paths(
        table_type: str, feed_info_path: str
    ) -> List[str]:
        """
        instead of mocking up s3 responses, just rewrite this method and
        monkeypatch it
        """
        logging.debug("input %s", feed_info_path)
        table_dir = feed_info_path.replace("FEED_INFO", table_type)
        files = [os.path.join(table_dir, f) for f in os.listdir(table_dir)]
        logging.debug("Mock Static Parquet Paths: %s", files)
        return files

    monkeypatch.setattr(
        "lib.l0_gtfs_static_table.get_static_parquet_paths",
        mock_get_static_parquet_paths,
    )

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


def test_gtfs_rt_processing(db_manager: DatabaseManager) -> None:
    """
    test that vehicle position and trip updates files can be consumed properly
    """
    db_manager.truncate_table(VehicleEvents)

    grouped_files = get_gtfs_rt_paths(db_manager)
    assert len(grouped_files) == 2

    for files in grouped_files:
        vp_files = files["vp_paths"]
        for path in files["vp_paths"]:
            assert "RT_VEHICLE_POSITIONS" in path

        # check that we can load the parquet file into a dataframe correctly
        positions = get_vp_dataframe(vp_files)
        position_size = positions.shape[0]
        assert positions.shape[1] == 9

        # check that the types can be set correctly
        positions = transform_vp_dtypes(positions)
        assert positions.shape[1] == 9
        assert position_size == positions.shape[0]

        # check that it can be combined with the static schedule
        positions = join_vp_with_gtfs_static(positions, db_manager)
        assert positions.shape[1] == 11
        assert position_size == positions.shape[0]

        positions = transform_vp_timestamps(positions)
        assert positions.shape[1] == 13
        assert positions.shape[0] < position_size

        tu_files = files["tu_paths"]
        trip_updates = get_and_unwrap_tu_dataframe(tu_files)
        trip_update_size = trip_updates.shape[0]
        assert trip_updates.shape[1] == 12

        trip_updates = join_tu_with_gtfs_static(trip_updates, db_manager)
        assert trip_updates.shape[1] == 14
        assert trip_update_size == trip_updates.shape[0]

        trip_updates = reduce_trip_updates(trip_updates)
        assert trip_update_size > trip_updates.shape[0]

        events = collapse_events(positions, trip_updates)

        expected_columns = {
            "pk_id",
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
            "stop_sequence",
            "stop_id",
            "parent_station",
            "trip_stop_hash",
            "vp_move_timestamp",
            "vp_stop_timestamp",
            "tu_stop_timestamp",
            "fk_static_timestamp",
        }
        actual_columns = set(events.columns)
        missing_columns = actual_columns - expected_columns
        assert len(missing_columns) == 0

        upload_to_database(events, db_manager)
        break