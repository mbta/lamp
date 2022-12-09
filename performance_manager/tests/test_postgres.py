import logging
import os
from typing import List, Iterator

import pytest
from _pytest.monkeypatch import MonkeyPatch

import sqlalchemy as sa

from lib.postgres_utils import (
    DatabaseManager,
)

from lib.postgres_schema import (
    MetadataLog,
    StaticTrips,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticCalendar,
    VehiclePositionEvents,
)

from lib import (
    process_dwell_travel_times,
    process_full_trip_events,
    process_headways,
    process_static_tables,
    process_trip_updates,
    process_vehicle_positions,
)

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
    seed the files in our test files filesystem into the metadata table
    """
    with db_manager.session.begin() as session:
        metadata_rows = session.query(MetadataLog).count()
        if metadata_rows != 0:
            return

    paths = []

    for root, _, files in os.walk(LAMP_DIR):
        for file in files:
            path = os.path.join(root, file)
            if (
                "FEED_INFO" in path
                or "RT_VEHICLE_POSITIONS" in path
                or "RT_TRIP_UPDATES" in path
            ):
                paths.append({"path": path})

    logging.info("Seeding %s paths", len(paths))

    with db_manager.session.begin() as session:
        session.execute(sa.insert(MetadataLog.__table__), paths)

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

    assert len(unprocessed_static_schedules) == 1

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
    last_count = 200
    count = 0
    while True:
        if count > 1:
            break

        sql_list = db_manager.select_as_list(
            sa.select(MetadataLog.path).where(
                MetadataLog.processed == sa.false()
            )
        )
        unprocessed_paths = [o["path"] for o in sql_list]

        current_count = len(unprocessed_paths)
        if current_count == 0:
            break

        assert current_count < last_count, unprocessed_paths

        for path in unprocessed_paths:
            assert "RT_VEHICLE_POSITIONS" in path or "RT_TRIP_UPDATES" in path

        process_vehicle_positions(db_manager)
        process_trip_updates(db_manager)
        process_full_trip_events(db_manager)
        process_dwell_travel_times(db_manager)
        process_headways(db_manager)

        with db_manager.session.begin() as session:
            session.query(VehiclePositionEvents).count()

        count += 1
