import logging
import os
from typing import Iterator, List

import pytest
import sqlalchemy as sa
from _pytest.monkeypatch import MonkeyPatch
from lib import process_static_tables
from lib.l0_rt_trip_updates import (
    get_and_unwrap_tu_dataframe,
    join_tu_with_gtfs_static,
    merge_tu_with_events,
    update_and_insert_db_events,
)
from lib.l0_rt_vehicle_positions import (
    get_vp_dataframe,
    insert_db_events,
    join_vp_with_gtfs_static,
    merge_with_overlapping_events,
    split_events,
    transform_vp_dtypes,
    transform_vp_timestamps,
    update_db_events,
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
    return DatabaseManager()


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


def test_vp_processing(db_manager: DatabaseManager) -> None:
    """
    test that vehicle position and trip updates files can be consumed properly
    """
    db_manager.truncate_table(VehicleEvents)

    sql_list = db_manager.select_as_list(
        sa.select(MetadataLog.path).where(MetadataLog.processed == sa.false())
    )
    unprocessed_paths = [
        o["path"] for o in sql_list if "RT_VEHICLE_POSITIONS" in o["path"]
    ]

    first_pass = True
    for path in unprocessed_paths:
        # check that the path is the right type
        assert "RT_VEHICLE_POSITIONS" in path

        # check that we can load the parquet file into a dataframe correctly
        positions = get_vp_dataframe(path)
        position_size = positions.shape[0]
        assert positions.shape[1] == 9

        # check that the types can be set correctly
        positions = transform_vp_dtypes(positions)
        assert positions.shape[1] == 9
        assert position_size == positions.shape[0]

        # check that it can be combined with the static schedule
        positions = join_vp_with_gtfs_static(positions, db_manager)
        found_keys = positions[positions["fk_static_timestamp"].notna()].shape[
            0
        ]
        assert found_keys != 0
        assert positions.shape[1] == 11
        assert position_size == positions.shape[0]

        new_events = transform_vp_timestamps(positions)
        assert new_events.shape[1] == 13
        assert new_events.shape[0] < position_size

        new_and_old_events = merge_with_overlapping_events(
            new_events, db_manager
        )

        if first_pass:
            assert new_and_old_events.shape[0] == new_events.shape[0]
        else:
            assert new_and_old_events.shape[0] >= new_events.shape[0]

        (update_events, insert_events) = split_events(new_and_old_events)

        update_db_events(update_events, db_manager)
        insert_db_events(insert_events, db_manager)

        first_pass = False


def test_tu_processing(db_manager: DatabaseManager) -> None:
    """
    test that trip updates are processed correctly
    """
    sql_list = db_manager.select_as_list(
        sa.select(MetadataLog.path).where(MetadataLog.processed == sa.false())
    )
    unprocessed_paths = [
        o["path"] for o in sql_list if "RT_TRIP_UPDATES" in o["path"]
    ]

    for path in unprocessed_paths:
        trip_updates = get_and_unwrap_tu_dataframe(path)
        assert trip_updates.shape[1] == 12

        trip_updates = join_tu_with_gtfs_static(trip_updates, db_manager)
        assert trip_updates.shape[1] == 14

        (update_events, insert_events) = merge_tu_with_events(
            trip_updates, db_manager
        )

        update_and_insert_db_events(update_events, insert_events, db_manager)
