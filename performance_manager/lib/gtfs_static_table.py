from typing import List, Optional
from dataclasses import dataclass

import os
import logging
import pandas
import numpy

import sqlalchemy as sa

from .s3_utils import read_parquet, file_list_from_s3
from .postgres_utils import MetadataLog, get_unprocessed_files, DatabaseManager
from .postgres_schema import (
    StaticFeedInfo,
    StaticTrips,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticCalendar,
)
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


def get_table_objects() -> List[StaticTableDetails]:
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

    return [
        feed_info,
        trips,
        routes,
        stops,
        stop_times,
        calendar,
    ]


def get_static_parquet_paths(table_type: str, feed_info_path: str) -> List[str]:
    """
    get static table parquet files from FEED_INFO path
    """
    springboard_bucket = os.environ["EXPORT_BUCKET"]
    static_prefix = feed_info_path.replace("FEED_INFO", table_type)
    static_prefix = static_prefix.replace(f"{springboard_bucket}/", "")
    return list(
        uri for uri in file_list_from_s3(springboard_bucket, static_prefix)
    )


def load_parquet_files(
    static_tables: List[StaticTableDetails], feed_info_path: str
) -> None:
    """
    get parquet paths to load from feed_info_path and load parquet files as
    dataframe into StaticTableDetails objects
    """
    for table in static_tables:
        paths_to_load = get_static_parquet_paths(
            table.table_name, feed_info_path
        )
        table.data_table = read_parquet(
            paths_to_load, columns=table.columns_to_pull
        )
        assert table.data_table.shape[0] > 0


def transform_data_tables(static_tables: List[StaticTableDetails]) -> None:
    """
    transform static gtfs schedule dataframe objects as required
    """
    for table in static_tables:
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
                ).astype(numpy.bool8)

        if table.time_to_seconds_cols is not None:
            for col in table.time_to_seconds_cols:
                table.data_table[col] = (
                    table.data_table[col]
                    .apply(start_time_to_seconds)
                    .astype("Int64")
                )


def insert_data_tables(
    static_tables: List[StaticTableDetails],
    db_manager: DatabaseManager,
) -> None:
    """
    insert static gtfs data tables into rds tables
    """
    for table in static_tables:
        db_manager.insert_dataframe(table.data_table, table.insert_table)


def process_static_tables(db_manager: DatabaseManager) -> None:
    """
    process gtfs static table files from metadataLog table
    """

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files("FEED_INFO", db_manager.get_session())

    for folder, folder_data in paths_to_load.items():
        ids = folder_data["ids"]
        folder = str(folder)

        static_tables = get_table_objects()

        try:
            load_parquet_files(static_tables, folder)
            transform_data_tables(static_tables)
            insert_data_tables(static_tables, db_manager)
        except Exception as e:
            logging.info("Error Processing Static GTFS Schedules")
            logging.exception(e)
        else:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            db_manager.execute(update_md_log)
