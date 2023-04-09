import os
import pathlib
from dataclasses import dataclass
from typing import List, Optional, Dict

import numpy
import pandas
import sqlalchemy as sa
from lamp_py.aws.ecs import check_for_sigterm
from lamp_py.aws.s3 import file_list_from_s3, read_parquet
from lamp_py.postgres.postgres_schema import (
    MetadataLog,
    StaticCalendar,
    StaticFeedInfo,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    StaticCalendarDates,
)
from lamp_py.postgres.postgres_utils import (
    DatabaseManager,
    get_unprocessed_files,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import start_time_to_seconds


@dataclass
class StaticTableDetails:
    """
    Container class for loading static gtfs schedule tables
    """

    table_name: str
    insert_table: sa.sql.schema.Table
    columns_to_pull: List[str]
    int64_cols: Optional[List[str]] = None
    bool_cols: Optional[List[str]] = None
    time_to_seconds_cols: Optional[List[str]] = None
    data_table: pandas.DataFrame = pandas.DataFrame()


def get_table_objects() -> Dict[str, StaticTableDetails]:
    """
    getter function for clean list of gtfs static table details objects
    """
    feed_info = StaticTableDetails(
        table_name="FEED_INFO",
        insert_table=StaticFeedInfo.__table__,
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
    )

    trips = StaticTableDetails(
        table_name="TRIPS",
        insert_table=StaticTrips.__table__,
        columns_to_pull=[
            "route_id",
            "service_id",
            "trip_id",
            "direction_id",
            "timestamp",
        ],
        int64_cols=[
            "timestamp",
        ],
        bool_cols=["direction_id"],
    )

    routes = StaticTableDetails(
        table_name="ROUTES",
        insert_table=StaticRoutes.__table__,
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
    )

    stops = StaticTableDetails(
        table_name="STOPS",
        insert_table=StaticStops.__table__,
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
    )

    stop_times = StaticTableDetails(
        table_name="STOP_TIMES",
        insert_table=StaticStopTimes.__table__,
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
    )

    calendar = StaticTableDetails(
        table_name="CALENDAR",
        insert_table=StaticCalendar.__table__,
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
    )

    calendar_dates = StaticTableDetails(
        table_name="CALENDAR_DATES",
        insert_table=StaticCalendarDates.__table__,
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
    )

    return {
        "feed_info": feed_info,
        "trips": trips,
        "routes": routes,
        "stops": stops,
        "stop_times": stop_times,
        "calendar": calendar,
        "calendar_dates": calendar_dates,
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
        table.data_table = read_parquet(
            paths_to_load, columns=table.columns_to_pull
        )
        assert table.data_table.shape[0] > 0


def transform_data_tables(static_tables: Dict[str, StaticTableDetails]) -> None:
    """
    transform static gtfs schedule dataframe objects as required
    """
    for table in static_tables.values():
        table.data_table = table.data_table.drop_duplicates()

        if table.int64_cols is not None:
            for col in table.int64_cols:
                table.data_table[col] = pandas.to_numeric(
                    table.data_table[col]
                ).astype("Int64")

        if table.bool_cols is not None:
            for col in table.bool_cols:
                table.data_table[col] = numpy.where(
                    table.data_table[col] == 1, True, False
                ).astype(numpy.bool_)

        if table.time_to_seconds_cols is not None:
            for col in table.time_to_seconds_cols:
                table.data_table[col] = (
                    table.data_table[col]
                    .apply(start_time_to_seconds)
                    .astype("Int64")
                )

        table.data_table = table.data_table.fillna(numpy.nan).replace(
            [numpy.nan], [None]
        )
        table.data_table = table.data_table.replace([""], [None])


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

    process_logger.add_metadata(
        stop_times_after_row_count=stop_times.shape[0],
    )
    process_logger.log_complete()


def insert_data_tables(
    static_tables: Dict[str, StaticTableDetails],
    db_manager: DatabaseManager,
) -> None:
    """
    insert static gtfs data tables into rds tables
    """
    for table in static_tables.values():
        db_manager.insert_dataframe(table.data_table, table.insert_table)


def process_static_tables(db_manager: DatabaseManager) -> None:
    """
    process gtfs static table files from metadataLog table
    """
    process_logger = ProcessLogger(
        "l0_tables_loader", table_type="static_schedule"
    )
    process_logger.log_start()

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files("FEED_INFO", db_manager)
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
            insert_data_tables(static_tables, db_manager)

            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            db_manager.execute(update_md_log)
            individual_logger.log_complete()
        except Exception as exception:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1, process_fail=1)
            )
            db_manager.execute(update_md_log)
            individual_logger.log_failure(exception)

    process_logger.log_complete()
