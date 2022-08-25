from typing import List, Optional

import logging
import pandas
import numpy

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from .s3_utils import read_parquet, file_list_from_s3
from .postgres_utils import MetadataLog, get_unprocessed_files
from .postgres_schema import (
    StaticFeedInfo,
    StaticTrips,
    StaticRoutes,
    StaticStops,
    StaticStopTimes,
    StaticCalendar,
)
from .gtfs_utils import start_time_to_seconds


def get_static_parquet_paths(table_type: str, feed_info_path: str) -> List[str]:
    """
    get static table parquet files from FEED_INFO path
    """
    springboard_bucket = "mbta-ctd-dataplatform-dev-springboard"
    static_prefix = feed_info_path.replace("FEED_INFO", table_type)
    static_prefix = static_prefix.replace(f"{springboard_bucket}/", "")
    return list(
        uri for uri in file_list_from_s3(springboard_bucket, static_prefix)
    )


# pylint: disable=too-many-arguments
def get_and_insert_static_tables(
    table_name: str,
    feed_info_path: str,
    session: sa.orm.session.sessionmaker,
    insert_table: str,
    columns_to_pull: List[str],
    int64_cols: Optional[List[str]] = None,
    bool_cols: Optional[List[str]] = None,
    time_to_seconds_cols: Optional[List[str]] = None,
) -> None:
    """
    common static table processing and insert logic
    """
    paths_to_load = get_static_parquet_paths(table_name, feed_info_path)

    data_table = read_parquet(paths_to_load, columns=columns_to_pull)

    assert data_table.shape[0] > 0

    data_table = data_table.drop_duplicates()

    if int64_cols is not None:
        for col in int64_cols:
            data_table[col] = pandas.to_numeric(data_table[col]).astype("Int64")

    if bool_cols is not None:
        for col in bool_cols:
            data_table[col] = numpy.where(
                data_table[col] == 1, True, False
            ).astype(numpy.bool8)

    if time_to_seconds_cols is not None:
        for col in time_to_seconds_cols:
            data_table[col] = (
                data_table[col].apply(start_time_to_seconds).astype("Int64")
            )

    with session.begin() as cursor:  # type: ignore
        cursor.execute(
            sa.insert(insert_table), data_table.to_dict(orient="records")
        )


# pylint: enable=too-many-arguments


def process_feed_info(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load feed_info table
    """
    table_name = "FEED_INFO"
    insert_table = StaticFeedInfo.__table__
    columns_to_pull = [
        "feed_start_date",
        "feed_end_date",
        "feed_version",
        "timestamp",
    ]
    int64_cols = [
        "feed_start_date",
        "feed_end_date",
        "timestamp",
    ]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
    )


def process_trips(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load trips table
    """
    table_name = "TRIPS"
    insert_table = StaticTrips.__table__
    columns_to_pull = [
        "route_id",
        "service_id",
        "trip_id",
        "direction_id",
        "timestamp",
    ]
    int64_cols = [
        "timestamp",
    ]
    bool_cols = ["direction_id"]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
        bool_cols=bool_cols,
    )


def process_routes(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load routes table
    """
    table_name = "ROUTES"
    insert_table = StaticRoutes.__table__
    columns_to_pull = [
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
    ]
    int64_cols = [
        "agency_id",
        "route_type",
        "route_sort_order",
        "timestamp",
    ]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
    )


def process_stops(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load stops table
    """
    table_name = "STOPS"
    insert_table = StaticStops.__table__
    columns_to_pull = [
        "stop_id",
        "stop_name",
        "stop_desc",
        "platform_code",
        "platform_name",
        "parent_station",
        "timestamp",
    ]
    int64_cols = [
        "timestamp",
    ]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
    )


def process_stop_times(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load stop_times table
    """
    table_name = "STOP_TIMES"
    insert_table = StaticStopTimes.__table__
    columns_to_pull = [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "timestamp",
    ]
    int64_cols = [
        "stop_sequence",
        "timestamp",
    ]
    time_to_seconds_cols = [
        "arrival_time",
        "departure_time",
    ]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
        time_to_seconds_cols=time_to_seconds_cols,
    )


def process_calendar(
    feed_info_path: str, session: sa.orm.session.sessionmaker
) -> None:
    """
    load calendar table
    """
    table_name = "CALENDAR"
    insert_table = StaticCalendar.__table__
    columns_to_pull = [
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
    ]
    int64_cols = [
        "start_date",
        "end_date",
        "timestamp",
    ]
    bool_cols = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    get_and_insert_static_tables(
        table_name,
        feed_info_path,
        session,
        insert_table,
        columns_to_pull=columns_to_pull,
        int64_cols=int64_cols,
        bool_cols=bool_cols,
    )


def process_static_tables(sql_session: sessionmaker) -> None:
    """
    process gtfs static table files from metadataLog table
    """

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files("FEED_INFO", sql_session)

    for folder, folder_data in paths_to_load.items():
        ids = folder_data["ids"]
        folder = str(folder)

        try:
            process_feed_info(folder, sql_session)
            process_trips(folder, sql_session)
            process_routes(folder, sql_session)
            process_stops(folder, sql_session)
            process_stop_times(folder, sql_session)
            process_calendar(folder, sql_session)
        except Exception as e:
            logging.info("Error Processing Trip Updates")
            logging.exception(e)
        else:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            with sql_session.begin() as cursor:  # type: ignore
                cursor.execute(update_md_log)
