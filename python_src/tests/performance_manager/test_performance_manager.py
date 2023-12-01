import logging
import os
import pathlib
from functools import lru_cache
from typing import (
    Dict,
    Iterator,
    List,
    Optional,
    Callable,
)

import pandas
import pytest
import sqlalchemy as sa
from _pytest.monkeypatch import MonkeyPatch
from pyarrow import Table

from lamp_py.performance_manager.flat_file import write_flat_files
from lamp_py.performance_manager.l0_gtfs_static_load import (
    process_static_tables,
)
from lamp_py.performance_manager.l0_gtfs_rt_events import (
    combine_events,
    get_gtfs_rt_paths,
    process_gtfs_rt_files,
    build_temp_events,
    update_events_from_temp,
)
from lamp_py.performance_manager.l0_rt_trip_updates import (
    get_and_unwrap_tu_dataframe,
    reduce_trip_updates,
)
from lamp_py.performance_manager.l0_rt_vehicle_positions import (
    get_vp_dataframe,
    transform_vp_datatypes,
    transform_vp_timestamps,
    process_vp_files,
)
from lamp_py.postgres.rail_performance_manager_schema import (
    MetadataLog,
    StaticCalendar,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    StaticCalendarDates,
    VehicleEvents,
    VehicleTrips,
    StaticDirections,
    StaticRoutePatterns,
)
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.alembic_migration import (
    alembic_upgrade_to_head,
    alembic_downgrade_to_base,
)

from lamp_py.performance_manager.gtfs_utils import (
    add_static_version_key_column,
    add_parent_station_column,
    rail_routes_from_filepath,
)

from ..test_resources import springboard_dir, test_files_dir, csv_to_vp_parquet


@lru_cache
def test_files() -> List[str]:
    """
    collapse all of the files in the lamp test files dir into a list
    """
    paths = []

    for root, _, files in os.walk(springboard_dir):
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
        logging.warning("already bootstrapped")
    else:
        env_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".env"
        )
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
    db_name = os.getenv("ALEMBIC_DB_NAME", "performance_manager")
    alembic_downgrade_to_base(db_name)
    alembic_upgrade_to_head(db_name)
    return db_manager


@pytest.fixture(autouse=True, name="s3_patch")
def fixture_s3_patch(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """
    insert a monkeypatch over s3 related functions. our tests use local
    files instead of s3, so we read these files differently
    """

    def mock__get_static_parquet_paths(
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
        "lamp_py.performance_manager.l0_gtfs_static_load.get_static_parquet_paths",
        mock__get_static_parquet_paths,
    )

    # pylint: disable=R0913
    # pylint too many arguments (more than 5)
    def mock__write_parquet_file(
        table: Table,
        file_type: str,
        s3_dir: str,
        partition_cols: List[str],
        visitor_func: Optional[Callable[..., None]] = None,
        basename_template: Optional[str] = None,
    ) -> None:
        """
        this will be called when writing the flat file parquet to s3
        """
        # check that only that flat file is being written
        assert file_type == "flat_rail_performance"
        assert visitor_func is None
        assert "lamp" in s3_dir
        assert "flat_file" in s3_dir

        # check that the service date is right in the filename
        assert basename_template == "2023-05-08-rail-performance-{i}.parquet"

        # check that the shape is good, rows count is precalculated
        rows, columns = table.shape
        assert columns == 30
        assert rows == 4310

        # check that partitioned columns behave appropriately
        for partition in partition_cols:
            assert partition in table.column_names
            assert len(table[partition].unique()) == 1

        # check that these keys have values throughout the file
        must_have_keys = [
            "stop_id",
            "parent_station",
            "route_id",
            "direction_id",
            "start_time",
            "vehicle_id",
            "trip_id",
            "vehicle_label",
            "year",
            "month",
            "day",
            "service_date",
        ]

        for key in must_have_keys:
            assert True not in table[key].is_null(
                nan_is_null=True
            ), f"{key} has null values"

    # pylint: enable=R0913

    monkeypatch.setattr(
        "lamp_py.performance_manager.flat_file.write_parquet_file",
        mock__write_parquet_file,
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

    db_manager.truncate_table(StaticTrips, restart_identity=True)
    db_manager.truncate_table(StaticRoutes, restart_identity=True)
    db_manager.truncate_table(StaticStops, restart_identity=True)
    db_manager.truncate_table(StaticStopTimes, restart_identity=True)
    db_manager.truncate_table(StaticCalendar, restart_identity=True)
    db_manager.truncate_table(StaticCalendarDates, restart_identity=True)
    db_manager.truncate_table(StaticDirections, restart_identity=True)
    db_manager.truncate_table(StaticRoutePatterns, restart_identity=True)

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
        StaticTrips: 11709,
        StaticRoutes: 24,
        StaticStops: 9743,
        StaticStopTimes: 186618,
        StaticCalendar: 102,
        StaticCalendarDates: 85,
        StaticDirections: 378,
        StaticRoutePatterns: 141,
    }

    with db_manager.session.begin() as session:
        for table, should_count in row_counts.items():
            actual_count = session.query(table).count()
            tablename = table.__tablename__
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


# pylint: disable=R0915
# pylint Too many statements (51/50) (too-many-statements)
def test_gtfs_rt_processing(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test that vehicle position and trip updates files can be consumed properly
    """
    caplog.set_level(logging.INFO)
    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)

    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        file
        for file in test_files()
        if ("RT_VEHICLE_POSITIONS" in file or "RT_TRIP_UPDATES" in file)
        and ("hour=12" in file or "hour=13" in file)
    ]
    db_manager.add_metadata_paths(paths)

    for files in get_gtfs_rt_paths(db_manager):
        for path in files["vp_paths"]:
            assert "RT_VEHICLE_POSITIONS" in path

        # check that we can load the parquet file into a dataframe correctly
        route_ids = rail_routes_from_filepath(files["vp_paths"], db_manager)
        positions = get_vp_dataframe(files["vp_paths"], route_ids)
        position_size = positions.shape[0]
        assert positions.shape[1] == 12

        # check that the types can be set correctly
        positions = transform_vp_datatypes(positions)
        assert positions.shape[1] == 12
        assert position_size == positions.shape[0]

        # check that it can be combined with the static schedule
        positions = add_static_version_key_column(positions, db_manager)
        assert positions.shape[1] == 13
        assert position_size == positions.shape[0]

        positions = add_parent_station_column(positions, db_manager)
        assert positions.shape[1] == 14
        assert position_size == positions.shape[0]

        positions = transform_vp_timestamps(positions)
        assert positions.shape[1] == 14
        assert position_size > positions.shape[0]

        trip_updates = get_and_unwrap_tu_dataframe(files["tu_paths"], route_ids)
        trip_update_size = trip_updates.shape[0]
        assert trip_updates.shape[1] == 9

        # check that it can be combined with the static schedule
        trip_updates = add_static_version_key_column(trip_updates, db_manager)
        assert trip_updates.shape[1] == 10
        assert trip_update_size == trip_updates.shape[0]

        trip_updates = add_parent_station_column(trip_updates, db_manager)
        assert trip_updates.shape[1] == 11
        assert trip_update_size == trip_updates.shape[0]

        trip_updates = reduce_trip_updates(trip_updates)
        assert trip_update_size > trip_updates.shape[0]

        events = combine_events(positions, trip_updates)

        ve_columns = [c.key for c in VehicleEvents.__table__.columns]
        # pm_trip_id and updated_on are handled by postgres
        # trip_id is pulled from parquet but not inserted into vehicle_events table
        expected_columns = set(ve_columns) - {
            "pm_event_id",
            "previous_trip_stop_pm_event_id",
            "next_trip_stop_pm_event_id",
            "updated_on",
            "pm_trip_id",
            "travel_time_seconds",
            "dwell_time_seconds",
            "headway_trunk_seconds",
            "headway_branch_seconds",
            "canonical_stop_sequence",
            "sync_stop_sequence",
        }
        expected_columns.add("trip_id")
        expected_columns.add("vehicle_label")
        expected_columns.add("vehicle_consist")
        expected_columns.add("direction_id")
        expected_columns.add("route_id")
        expected_columns.add("start_time")
        expected_columns.add("vehicle_id")
        expected_columns.add("static_version_key")
        assert len(expected_columns) == len(events.columns)

        missing_columns = set(events.columns) - expected_columns
        assert len(missing_columns) == 0

        build_temp_events(events, db_manager)
        update_events_from_temp(db_manager)

    write_flat_files(db_manager)

    check_logs(caplog)


# pylint: enable=R0915


def test_vp_only(
    db_manager: DatabaseManager, caplog: pytest.LogCaptureFixture
) -> None:
    """
    test the vehicle positions can be updated without trip updates
    """
    caplog.set_level(logging.INFO)

    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        p
        for p in test_files()
        if "RT_VEHICLE_POSITIONS" in p and ("hourt=12" in p or "hour=13" in p)
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

    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [
        p
        for p in test_files()
        if "RT_TRIP_UPDATES" in p and ("hourt=12" in p or "hour=13" in p)
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

    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    paths = [p for p in test_files() if "hourt=12" in p or "hour=13" in p]
    db_manager.add_metadata_paths(paths)

    process_gtfs_rt_files(db_manager)

    check_logs(caplog)


def test_missing_start_time(
    db_manager: DatabaseManager,
    caplog: pytest.LogCaptureFixture,
    tmp_path: pathlib.Path,
) -> None:
    """
    test the vehicle positions can be updated without trip updates
    """
    # capture the info logs to check after running the test
    caplog.set_level(logging.INFO)

    # clear out old data from the database
    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    # create a new parquet file from ths missing start times csv and add it to
    # the metadata table for processing
    csv_file = os.path.join(test_files_dir, "vp_missing_start_time.csv")
    parquet_folder = tmp_path.joinpath(
        "RT_VEHICLE_POSITIONS/year=2023/month=5/day=8/hour=11"
    )
    parquet_folder.mkdir(parents=True)
    parquet_file = str(parquet_folder.joinpath("flat_file.parquet"))
    csv_to_vp_parquet(csv_file, parquet_file)
    db_manager.add_metadata_paths(
        [
            parquet_file,
        ]
    )

    # process the parquet file
    process_gtfs_rt_files(db_manager)

    # check that all trips have an int convertible start time that is in
    # seconds after midnight.
    start_time_query = sa.select(VehicleTrips.pm_trip_id).where(
        VehicleTrips.start_time.is_(None),
        VehicleTrips.start_time < 0,
        # 129600 is a day and a half.
        VehicleTrips.start_time > 129600,
    )

    weird_start_times = db_manager.select_as_dataframe(start_time_query)
    assert weird_start_times.shape[0] == 0

    # there is an added trip in the csv data who's first move time is 1683547198
    # or 7:59:58 AM. that is 25200 + 3540 + 58 = 28798 seconds after midnight.
    added_trip_start_time = db_manager.select_as_list(
        sa.select(VehicleTrips.start_time).where(
            VehicleTrips.trip_id == "ADDED-1581518546"
        )
    )
    assert len(added_trip_start_time) == 1
    assert added_trip_start_time[0]["start_time"] == 28798

    # check that there are no errors in the logs and that each start log has a
    # completion log
    check_logs(caplog)


def test_process_vp_files(
    db_manager: DatabaseManager,
    caplog: pytest.LogCaptureFixture,
    tmp_path: pathlib.Path,
) -> None:
    """
    check whole flat file
    """
    caplog.set_level(logging.INFO)

    csv_file = os.path.join(test_files_dir, "vehicle_positions_flat_input.csv")
    parquet_folder = tmp_path.joinpath(
        "RT_VEHICLE_POSITIONS/year=2023/month=5/day=8/hour=11"
    )
    parquet_folder.mkdir(parents=True)
    parquet_file = str(parquet_folder.joinpath("flat_file.parquet"))

    csv_to_vp_parquet(csv_file, parquet_file)

    result_df = process_vp_files(parquet_file, db_manager)

    result_dtypes = {
        "service_date": "int64",
        "route_id": "string",
        "trip_id": "string",
        "parent_station": "string",
        "stop_id": "string",
        "vehicle_id": "string",
        "vehicle_label": "string",
        "vehicle_consist": "string",
        "vp_move_timestamp": "Int64",
        "vp_stop_timestamp": "Int64",
        "start_time": "Int64",
    }

    csv_result_df = pandas.read_csv(
        os.path.join(test_files_dir, "process_vp_files_flat_out.csv"),
        dtype=result_dtypes,
    )

    sort_by = list(csv_result_df.columns)

    csv_result_df = csv_result_df.sort_values(
        by=sort_by,
        ignore_index=True,
    )
    result_df = result_df.astype(dtype=result_dtypes).sort_values(
        by=sort_by,
        ignore_index=True,
    )

    column_exceptions = []
    for column in csv_result_df.columns:
        try:
            pandas.testing.assert_series_equal(
                result_df[column], csv_result_df[column]
            )
        except Exception as exception:
            logging.error(
                "Pipeline values in %s column do not match process_vp_files_flat_out.csv CSV file",
                column,
            )
            column_exceptions.append(exception)

    # will only raise one column exception, but error logging will print all columns with issues
    if column_exceptions:
        raise column_exceptions[0]

    check_logs(caplog)


def test_whole_table(
    db_manager: DatabaseManager,
    caplog: pytest.LogCaptureFixture,
    tmp_path: pathlib.Path,
) -> None:
    """
    check whole flat file
    """
    caplog.set_level(logging.INFO)

    db_manager.truncate_table(VehicleEvents, restart_identity=True)
    db_manager.truncate_table(VehicleTrips, restart_identity=True)
    db_manager.execute(
        sa.delete(MetadataLog.__table__).where(
            ~MetadataLog.path.contains("FEED_INFO")
        )
    )

    csv_file = os.path.join(test_files_dir, "vehicle_positions_flat_input.csv")
    parquet_folder = tmp_path.joinpath(
        "RT_VEHICLE_POSITIONS/year=2023/month=5/day=8/hour=11"
    )
    parquet_folder.mkdir(parents=True)
    parquet_file = str(parquet_folder.joinpath("flat_file.parquet"))

    csv_to_vp_parquet(csv_file, parquet_file)

    db_manager.add_metadata_paths(
        [
            parquet_file,
        ]
    )

    process_gtfs_rt_files(db_manager)

    result_select = (
        sa.select(
            VehicleEvents.stop_sequence,
            VehicleEvents.stop_id,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleTrips.direction_id,
            VehicleTrips.direction,
            VehicleTrips.direction_destination,
            VehicleTrips.route_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
            VehicleTrips.service_date,
            VehicleTrips.start_time,
            VehicleTrips.vehicle_id,
            VehicleTrips.stop_count,
            VehicleTrips.trip_id,
            VehicleTrips.vehicle_label,
            VehicleTrips.static_trip_id_guess,
            VehicleEvents.dwell_time_seconds,
            VehicleEvents.travel_time_seconds,
            VehicleEvents.headway_branch_seconds,
            VehicleEvents.headway_trunk_seconds,
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleTrips.pm_trip_id == VehicleEvents.pm_trip_id,
            isouter=True,
        )
    )
    result_dtypes = {
        "stop_id": "string",
        "parent_station": "string",
        "direction": "string",
        "direction_destination": "string",
        "route_id": "string",
        "branch_route_id": "string",
        "trunk_route_id": "string",
        "vehicle_id": "string",
        "trip_id": "string",
        "vehicle_label": "string",
        "vp_move_timestamp": "Int64",
        "vp_stop_timestamp": "Int64",
        "dwell_time_seconds": "Int64",
        "travel_time_seconds": "Int64",
        "headway_branch_seconds": "Int64",
        "headway_trunk_seconds": "Int64",
        "static_trip_id_guess": "Int64",
        "start_time": "Int64",
    }
    sort_by = [
        "route_id",
        "direction_id",
        "stop_sequence",
        "stop_id",
        "service_date",
        "start_time",
    ]
    db_result_df = db_manager.select_as_dataframe(result_select)
    db_result_df = db_result_df.astype(dtype=result_dtypes).sort_values(
        by=sort_by,
        ignore_index=True,
    )

    csv_result_df = pandas.read_csv(
        os.path.join(test_files_dir, "pipeline_flat_out.csv"),
        dtype=result_dtypes,
    )
    csv_result_df = csv_result_df.sort_values(
        by=sort_by,
        ignore_index=True,
    )

    column_exceptions = []
    for column in csv_result_df.columns:
        try:
            pandas.testing.assert_series_equal(
                db_result_df[column], csv_result_df[column]
            )
        except Exception as exception:
            logging.error(
                "Pipeline values in %s column do not match pipeline_flat_out.csv CSV file",
                column,
            )
            column_exceptions.append(exception)

    # will only raise one column exception, but error logging will print all columns with issues
    if column_exceptions:
        raise column_exceptions[0]

    check_logs(caplog)
