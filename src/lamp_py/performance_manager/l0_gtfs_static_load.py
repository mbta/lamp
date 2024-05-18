import os
import pathlib
from dataclasses import dataclass
from typing import List, Optional, Dict

import numpy
import pandas
import pyarrow
import sqlalchemy as sa
from lamp_py.aws.ecs import check_for_sigterm
from lamp_py.aws.s3 import file_list_from_s3, read_parquet
from lamp_py.postgres.metadata_schema import MetadataLog
from lamp_py.postgres.rail_performance_manager_schema import (
    StaticCalendar,
    StaticFeedInfo,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    StaticCalendarDates,
    StaticDirections,
    StaticRoutePatterns,
)
from lamp_py.postgres.postgres_utils import (
    DatabaseManager,
    get_unprocessed_files,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.infinite_wait import infinite_wait

from .gtfs_utils import start_time_to_seconds

from .l0_gtfs_static_mod import modify_static_tables


@dataclass
class StaticTableColumns:
    """
    Container Class to hold conversion information for gtfs schedule tables
    """

    columns_to_pull: List[str]
    int64_cols: Optional[List[str]] = None
    bool_cols: Optional[List[str]] = None
    time_to_seconds_cols: Optional[List[str]] = None


@dataclass
class StaticTableDetails:
    """
    Container class for loading static gtfs schedule tables
    """

    table_name: str
    insert_table: sa.sql.schema.Table
    static_version_key_column: sa.sql.schema.Column
    column_info: StaticTableColumns
    data_table: pandas.DataFrame = pandas.DataFrame()
    allow_empty_dataframe: bool = False


def get_table_objects() -> Dict[str, StaticTableDetails]:
    """
    getter function for clean list of gtfs static table details objects
    """
    feed_info = StaticTableDetails(
        table_name="FEED_INFO",
        insert_table=StaticFeedInfo.__table__,
        static_version_key_column=StaticFeedInfo.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "feed_start_date",
                "feed_end_date",
                "feed_version",
                "timestamp",
            ],
            int64_cols=[
                "feed_start_date",
                "feed_end_date",
                "timestamp",
            ],
        ),
    )

    trips = StaticTableDetails(
        table_name="TRIPS",
        insert_table=StaticTrips.__table__,
        static_version_key_column=StaticTrips.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "route_id",
                "service_id",
                "trip_id",
                "direction_id",
                "block_id",
                "timestamp",
            ],
            int64_cols=[
                "timestamp",
            ],
            bool_cols=["direction_id"],
        ),
    )

    routes = StaticTableDetails(
        table_name="ROUTES",
        insert_table=StaticRoutes.__table__,
        static_version_key_column=StaticRoutes.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_desc",
                "route_type",
                "route_sort_order",
                "route_fare_class",
                "line_id",
                "timestamp",
            ],
            int64_cols=[
                "agency_id",
                "route_type",
                "route_sort_order",
                "timestamp",
            ],
        ),
    )

    stops = StaticTableDetails(
        table_name="STOPS",
        insert_table=StaticStops.__table__,
        static_version_key_column=StaticStops.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "stop_id",
                "stop_name",
                "stop_desc",
                "platform_code",
                "platform_name",
                "parent_station",
                "timestamp",
            ],
            int64_cols=[
                "timestamp",
            ],
        ),
    )

    stop_times = StaticTableDetails(
        table_name="STOP_TIMES",
        insert_table=StaticStopTimes.__table__,
        static_version_key_column=StaticStopTimes.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
                "timestamp",
            ],
            int64_cols=[
                "stop_sequence",
                "timestamp",
            ],
            time_to_seconds_cols=[
                "arrival_time",
                "departure_time",
            ],
        ),
    )

    calendar = StaticTableDetails(
        table_name="CALENDAR",
        insert_table=StaticCalendar.__table__,
        static_version_key_column=StaticCalendar.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
                "timestamp",
            ],
            int64_cols=[
                "start_date",
                "end_date",
                "timestamp",
            ],
            bool_cols=[
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ],
        ),
    )

    calendar_dates = StaticTableDetails(
        table_name="CALENDAR_DATES",
        insert_table=StaticCalendarDates.__table__,
        static_version_key_column=StaticCalendarDates.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "service_id",
                "date",
                "exception_type",
                "holiday_name",
                "timestamp",
            ],
            int64_cols=[
                "date",
                "exception_type",
                "timestamp",
            ],
        ),
        allow_empty_dataframe=True,
    )

    directions = StaticTableDetails(
        table_name="DIRECTIONS",
        insert_table=StaticDirections.__table__,
        static_version_key_column=StaticDirections.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "route_id",
                "direction_id",
                "direction",
                "direction_destination",
                "timestamp",
            ],
            int64_cols=[
                "timestamp",
            ],
            bool_cols=[
                "direction_id",
            ],
        ),
    )

    route_patterns = StaticTableDetails(
        table_name="ROUTE_PATTERNS",
        insert_table=StaticRoutePatterns.__table__,
        static_version_key_column=StaticRoutePatterns.static_version_key,
        column_info=StaticTableColumns(
            columns_to_pull=[
                "route_id",
                "direction_id",
                "route_pattern_typicality",
                "representative_trip_id",
                "timestamp",
            ],
            int64_cols=[
                "timestamp",
                "route_pattern_typicality",
            ],
            bool_cols=[
                "direction_id",
            ],
        ),
    )

    # this return order also dictates the order that tables are loaded into the RDS
    # the 'static_trips_create_branch_trunk' trigger requires that the `stop_times` table
    # be loaded prior to the `trips` table
    return {
        "feed_info": feed_info,
        "routes": routes,
        "stops": stops,
        "stop_times": stop_times,
        "trips": trips,
        "calendar": calendar,
        "calendar_dates": calendar_dates,
        "directions": directions,
        "route_patterns": route_patterns,
    }


def get_static_parquet_paths(table_type: str, feed_info_path: str) -> List[str]:
    """
    get static table parquet files from FEED_INFO path
    """
    springboard_bucket = os.environ["SPRINGBOARD_BUCKET"]
    static_prefix = feed_info_path.replace("FEED_INFO", table_type)
    static_prefix = static_prefix.replace(f"{springboard_bucket}/", "")
    return file_list_from_s3(springboard_bucket, static_prefix)


def load_parquet_files(
    static_tables: Dict[str, StaticTableDetails], feed_info_path: str
) -> None:
    """
    get parquet paths to load from feed_info_path and load parquet files as
    dataframe into StaticTableDetails objects
    """
    for table in static_tables.values():
        paths_to_load = get_static_parquet_paths(
            table.table_name, feed_info_path
        )
        try:
            table.data_table = read_parquet(
                paths_to_load, columns=table.column_info.columns_to_pull
            )
            assert table.data_table.shape[0] > 0
        except pyarrow.ArrowInvalid as exception:
            if table.allow_empty_dataframe is False:
                raise exception

            table.data_table = pandas.DataFrame(
                columns=table.column_info.columns_to_pull
            )


def transform_data_tables(static_tables: Dict[str, StaticTableDetails]) -> None:
    """
    transform static gtfs schedule dataframe objects as required
    """
    for table in static_tables.values():
        table.data_table = table.data_table.drop_duplicates()

        if table.column_info.int64_cols is not None:
            for col in table.column_info.int64_cols:
                table.data_table[col] = pandas.to_numeric(
                    table.data_table[col]
                ).astype("Int64")

        if table.column_info.bool_cols is not None:
            for col in table.column_info.bool_cols:
                table.data_table[col] = numpy.where(
                    table.data_table[col] == 1, True, False
                ).astype(numpy.bool_)

        if table.column_info.time_to_seconds_cols is not None:
            for col in table.column_info.time_to_seconds_cols:
                table.data_table[col] = (
                    table.data_table[col]
                    .apply(start_time_to_seconds)
                    .astype("Int64")
                )

        table.data_table = table.data_table.fillna(numpy.nan).replace(
            [numpy.nan], [None]
        )
        table.data_table = table.data_table.replace([""], [None])

        table.data_table = table.data_table.rename(
            columns={
                "timestamp": "static_version_key",
            }
        )


def drop_bus_records(static_tables: Dict[str, StaticTableDetails]) -> None:
    """
    remove bus records from "routes", "trips" and "stop_times" static tables
    """
    process_logger = ProcessLogger(
        "gtfs.remove_bus_records",
        stop_times_start_row_count=static_tables["stop_times"].data_table.shape[
            0
        ],
    )
    process_logger.log_start()

    # remove bus routes (route_type == 3) from routes table
    routes = static_tables["routes"].data_table
    routes = routes[(routes["route_type"] != 3)]
    # save non-bus route_id's to be joined with other dataframes
    no_bus_route_ids = routes[["route_id"]]

    # save new routes dataframe for RDS insertion
    static_tables["routes"].data_table = routes

    trips = static_tables["trips"].data_table
    # inner join trips on non-bus route_ids to remove bus trips
    trips = trips.merge(no_bus_route_ids, how="inner", on="route_id")
    # save non-bus trip_id's to be joined with other dataframes
    no_bus_trip_ids = trips[["trip_id"]]

    # save new trips dataframe for RDS insertion
    static_tables["trips"].data_table = trips

    stop_times = static_tables["stop_times"].data_table
    # inner join trips on non-bus trip_ids to remove bus stop times
    stop_times = stop_times.merge(no_bus_trip_ids, how="inner", on="trip_id")

    # save new stop_times dataframe for RDS insertion
    static_tables["stop_times"].data_table = stop_times

    static_tables["route_patterns"].data_table = static_tables[
        "route_patterns"
    ].data_table.merge(no_bus_route_ids, how="inner", on="route_id")

    process_logger.add_metadata(
        stop_times_after_row_count=stop_times.shape[0],
    )
    process_logger.log_complete()


def insert_data_tables(
    static_tables: Dict[str, StaticTableDetails],
    static_version_key: int,
    db_manager: DatabaseManager,
) -> None:
    """
    insert static gtfs data tables into rds tables and run a vacuum analyze
    afterwards to re-index
    """
    try:
        for table in static_tables.values():
            process_logger = ProcessLogger(
                "gtfs_insert", table_name=table.table_name
            )
            process_logger.log_start()

            if table.data_table.shape[0] > 0:
                db_manager.insert_dataframe(
                    table.data_table, table.insert_table
                )
                db_manager.vacuum_analyze(table.insert_table)
            process_logger.log_complete()
    except Exception as error:
        # if an error occurs in loading one of the tables, remove static data
        # from all tables matching the same static key. re-raise the error so
        # it can be properly logged.
        for table in static_tables.values():
            process_logger = ProcessLogger(
                "gtfs_clean", table_name=table.table_name
            )
            process_logger.log_start()
            delete_static = sa.delete(table.insert_table).where(
                table.static_version_key_column == static_version_key
            )
            db_manager.execute(delete_static)
            process_logger.log_complete()
        raise error


def process_static_tables(
    rpm_db_manager: DatabaseManager,
    md_db_manager: DatabaseManager,
) -> None:
    """
    process gtfs static table files from metadataLog table
    """
    process_logger = ProcessLogger(
        "l0_tables_loader", table_type="static_schedule"
    )
    process_logger.log_start()

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files("FEED_INFO", md_db_manager)
    process_logger.add_metadata(count_of_paths=len(paths_to_load))

    for folder_data in paths_to_load:
        check_for_sigterm()
        folder = str(pathlib.Path(folder_data["paths"][0]).parent)
        individual_logger = ProcessLogger(
            "l0_load_table", table_type="static_schedule", s3_path=folder
        )
        individual_logger.log_start()

        ids = folder_data["ids"]

        static_tables = get_table_objects()

        try:
            load_parquet_files(static_tables, folder)
            transform_data_tables(static_tables)
            drop_bus_records(static_tables)

            static_version_key = int(
                static_tables["feed_info"].data_table.loc[
                    0, "static_version_key"
                ]
            )

            insert_data_tables(
                static_tables, static_version_key, rpm_db_manager
            )
            modify_static_tables(static_version_key, rpm_db_manager)

            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(rail_pm_processed=True)
            )
            md_db_manager.execute(update_md_log)
            individual_logger.log_complete()
        except Exception as exception:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(rail_pm_processed=True, rail_pm_process_fail=True)
            )
            md_db_manager.execute(update_md_log)
            individual_logger.log_failure(exception)

            infinite_wait(reason=f"Error Loading Static File {folder}")

    process_logger.log_complete()
