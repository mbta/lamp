import logging
import os
from typing import Iterator, List

import pandas as pa
import pytest
import sqlalchemy as sa
from _pytest.monkeypatch import MonkeyPatch
from lib import (
    process_dwell_travel_times,
    process_full_trip_events,
    process_headways,
    process_static_tables,
    process_trip_updates,
    process_vehicle_positions,
)
from lib.postgres_schema import (
    DwellTimes,
    MetadataLog,
    StaticCalendar,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    VehiclePositionEvents,
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

    # assert len(unprocessed_static_schedules) == 1

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
    sql_list = db_manager.select_as_list(
        sa.select(MetadataLog.path).where(MetadataLog.processed == sa.false())
    )
    unprocessed_paths = [o["path"] for o in sql_list]

    for path in unprocessed_paths:
        assert "RT_VEHICLE_POSITIONS" in path or "RT_TRIP_UPDATES" in path

    process_vehicle_positions(db_manager)
    process_trip_updates(db_manager)
    process_full_trip_events(db_manager)

    process_dwell_travel_times(db_manager)
    process_headways(db_manager)

    with db_manager.session.begin() as session:
        vp_count = session.query(VehiclePositionEvents).count()
        assert vp_count > 0


def test_l0_vehicle_positions(db_manager: DatabaseManager) -> None:
    """
    check that the events vehicle positions table is hitting each of the stops that was found in the source parquet files.
    """
    stops_per_trip = {
        14400: {
            "in_station_events": [
                "70063",
                "70065",
                "70067",
                "70069",
                "70071",
                "70073",
                "70075",
                "70077",
                "70079",
                "70081",
                "70083",
                "70095",
                "70097",
                "70099",
                "70101",
                "70103",
            ],
            "in_transit_events": [
                "70063",
                "70065",
                "70067",
                "70069",
                "70071",
                "70073",
                "70075",
                "70077",
                "70079",
                "70081",
                "70083",
                "70095",
                "70097",
                "70099",
                "70101",
                "70103",
                "70105",
            ],
        },
        18968: {
            "in_station_events": [
                "Braintree-01",
                "70104",
                "70102",
                "70100",
                "70098",
                "70096",
                "70084",
                "70082",
                "70080",
                "70078",
                "70076",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
            ],
            "in_transit_events": [
                "70104",
                "70102",
                "70100",
                "70098",
                "70096",
                "70084",
                "70082",
                "70080",
                "70078",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
                "70061",
            ],
        },
        19022: {
            "in_station_events": [
                "70092",
                "70090",
                "70088",
                "70086",
                "70084",
                "70082",
                "70080",
                "70078",
                "70076",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
                "70061",
            ],
            "in_transit_events": [
                "70092",
                "70090",
                "70088",
                "70086",
                "70084",
                "70082",
                "70080",
                "70078",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
                "70061",
            ],
        },
        18960: {
            "in_station_events": [
                "70094",
                "70092",
                "70090",
                "70088",
                "70086",
                "70084",
                "70082",
                "70080",
                "70078",
                "70076",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
                "70061",
            ],
            "in_transit_events": [
                "70094",
                "70092",
                "70090",
                "70088",
                "70086",
                "70084",
                "70082",
                "70080",
                "70078",
                "70074",
                "70072",
                "70070",
                "70068",
                "70066",
                "70064",
                "70061",
            ],
        },
        19398: {
            "in_station_events": [
                "70063",
                "70065",
                "70067",
                "70069",
                "70071",
                "70073",
                "70075",
                "70077",
                "70079",
                "70081",
                "70083",
                "70085",
                "70087",
                "70089",
                "70091",
                "70093",
            ],
            "in_transit_events": [
                "70063",
                "70065",
                "70067",
                "70069",
                "70071",
                "70073",
                "70075",
                "70077",
                "70079",
                "70081",
                "70083",
                "70085",
                "70087",
                "70089",
                "70091",
                "70093",
            ],
        },
    }

    for start_time, trip_info in stops_per_trip.items():
        events = db_manager.select_as_dataframe(
            sa.select(
                VehiclePositionEvents.stop_id, VehiclePositionEvents.is_moving
            ).where(
                (VehiclePositionEvents.route_id == "Red")
                & (VehiclePositionEvents.start_time == start_time)
            )
        )

        for stop_id in trip_info["in_station_events"]:
            stop_events = events[events.stop_id == stop_id]
            stop_events = stop_events[~stop_events.is_moving]
            assert (
                len(stop_events) > 0
            ), f"missing stop event for {start_time} at {stop_id}"

        for stop_id in trip_info["in_transit_events"]:
            transit_events = events[events.stop_id == stop_id]
            transit_events = transit_events[transit_events.is_moving]
            assert (
                len(transit_events) > 0
            ), f"missing transit event for {start_time} towards {stop_id}"


def test_dwell_time_data(db_manager: DatabaseManager) -> None:
    """
    test that dwell times in the performance manager database match values that
    were precomputed from looking at parquet data
    """
    pre_computed_dwell_times = pa.DataFrame(
        [
            {"stop_id": "70092", "start_time": 24120, "dwell_time_seconds": 5},
            {"stop_id": "70092", "start_time": 19177, "dwell_time_seconds": 6},
            {"stop_id": "70074", "start_time": 19669, "dwell_time_seconds": 10},
            {"stop_id": "70065", "start_time": 23955, "dwell_time_seconds": 10},
            # {"stop_id": "70066", "start_time": 18968, "dwell_time": 29},
            # {"stop_id": "70066", "start_time": 18555, "dwell_time": 36},
            # {"stop_id": "70070", "start_time": 18960, "dwell_time": 38},
            # {"stop_id": "70074", "start_time": 22380, "dwell_time": 115},
        ]
    )

    computed_dwell_times = db_manager.select_as_dataframe(
        sa.select(
            VehiclePositionEvents.stop_id,
            VehiclePositionEvents.start_time,
            DwellTimes.dwell_time_seconds,
        )
        .join(
            VehiclePositionEvents,
            DwellTimes.fk_dwell_time_id == VehiclePositionEvents.pk_id,
        )
        .where(
            (DwellTimes.dwell_time_seconds != 0)
            & (VehiclePositionEvents.route_id == "Red")
        )
        .order_by(
            VehiclePositionEvents.stop_id, VehiclePositionEvents.start_time
        )
    )

    # sort the values and reset the index so that it can be compared against the precomputed dwll times
    computed_dwell_times = computed_dwell_times.sort_values(
        by=["dwell_time_seconds", "start_time", "stop_id"]
    ).reset_index(drop=True)

    pa.testing.assert_frame_equal(
        pre_computed_dwell_times, computed_dwell_times, check_like=True
    )
