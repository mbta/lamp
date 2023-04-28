import hashlib
from typing import Optional, Sequence

import numpy
import pandas
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    StaticFeedInfo,
    StaticStops,
    StaticRoutes,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger


def add_event_hash_column(
    df_to_hash: pandas.DataFrame,
    hash_column_name: str,
    expected_hash_columns: Sequence[str],
) -> pandas.DataFrame:
    """
    provide consistent hash values for category columns of gtfs-rt events
    """

    row_check = set(expected_hash_columns) - set(df_to_hash.columns)
    if len(row_check) > 0:
        raise IndexError(f"Dataframe is missing expected columns: {row_check}")

    # handle dataframe with no records
    if df_to_hash.shape[0] == 0:
        df_to_hash[hash_column_name] = None
        return df_to_hash

    # function to be used for hashing each record,
    # requires string as input returns raw bytes object
    def apply_func(record: str) -> str:
        return hashlib.md5(record.encode("utf8")).hexdigest()

    # vectorize apply_func so it can be used on numpy.ndarray object
    vectorized_function = numpy.vectorize(apply_func)

    # replace all "na" types values with python None to create consistent hash
    df_to_hash = df_to_hash.fillna(numpy.nan).replace([numpy.nan], [None])

    # convert rows of dataframe to concatenated string and apply vectorized
    # hashing function
    expected_hash_columns = sorted(expected_hash_columns)
    df_to_hash[hash_column_name] = vectorized_function(
        df_to_hash[list(expected_hash_columns)].astype(str).values.sum(axis=1)
    )

    return df_to_hash


def start_time_to_seconds(
    time: Optional[str],
) -> Optional[float]:
    """
    transform time string in HH:MM:SS format to seconds
    """
    if time is None:
        return time
    (hour, minute, second) = time.split(":")
    return int(hour) * 3600 + int(minute) * 60 + int(second)


def add_fk_static_timestamp_column(
    events_dataframe: pandas.DataFrame,
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    adds "fk_static_timestamp" column to dataframe

    using "fk_static_timestamp" column, events dataframe records may be joined to
    gtfs static record tables
    """
    process_logger = ProcessLogger(
        "add_fk_static_timestamp",
        row_count=events_dataframe.shape[0],
    )
    process_logger.log_start()

    # initialize fk_static_timestamp column
    events_dataframe["fk_static_timestamp"] = 0

    for date in events_dataframe["start_date"].unique():
        date = int(date)
        # "start_date" from events dataframe must be between "feed_start_date" and "feed_end_date" in StaticFeedInfo
        # "start_date" must also be less than or equal to "feed_active_date" in StaticFeedInfo
        # StaticFeedInfo, order by feed_active_date descending and created_on date descending
        # this should deal with multiple static schedules being issued on the same day
        # if this occurs we will use the latest issued schedule
        live_match_query = (
            sa.select(StaticFeedInfo.timestamp)
            .where(
                StaticFeedInfo.feed_start_date <= date,
                StaticFeedInfo.feed_end_date >= date,
                StaticFeedInfo.feed_active_date <= date,
            )
            .order_by(
                StaticFeedInfo.feed_active_date.desc(),
                StaticFeedInfo.created_on.desc(),
            )
            .limit(1)
        )

        # "feed_start_date" and "feed_end_date" are modified for archived GTFS Schedule files
        # If processing archived static schedules, these alternate rules must be used for matching
        # GTFS static to GTFS-RT data
        archive_match_query = (
            sa.select(StaticFeedInfo.timestamp)
            .where(
                StaticFeedInfo.feed_start_date <= date,
                StaticFeedInfo.feed_end_date >= date,
            )
            .order_by(
                StaticFeedInfo.feed_start_date.desc(),
                StaticFeedInfo.created_on.desc(),
            )
            .limit(1)
        )

        result = db_manager.select_as_list(live_match_query)

        # if live_match_query fails, attempt to look for a match using the archive method
        if len(result) == 0:
            result = db_manager.select_as_list(archive_match_query)

        # if this query does not produce a result, no static schedule info
        # exists for this trip update data, so the data
        # should not be processed until valid static schedule data exists
        if len(result) == 0:
            raise IndexError(
                f"StaticFeedInfo table has no matching schedule for start_date={date}"
            )

        start_date_mask = events_dataframe["start_date"] == date
        events_dataframe.loc[start_date_mask, "fk_static_timestamp"] = int(
            result[0]["timestamp"]
        )

    process_logger.log_complete()

    return events_dataframe


def add_parent_station_column(
    events_dataframe: pandas.DataFrame,
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    adds "parent_station" column to dataframe

    events_dataframe must have "fk_static_timestamp" and "stop_id" columns

    if "parent_station" value does not exist for a specific "stop_id", then
    "stop_id" is used as "parent_station"
    """
    process_logger = ProcessLogger(
        "add_parent_station",
        row_count=events_dataframe.shape[0],
    )
    process_logger.log_start()

    # handle dataframe with no rows
    if events_dataframe.shape[0] == 0:
        events_dataframe["parent_station"] = None
        process_logger.log_complete()
        return events_dataframe

    # unique list of "fk_static_timestamp" values for pulling parent stations
    static_timestamps = [
        int(timestamp)
        for timestamp in events_dataframe["fk_static_timestamp"].unique()
    ]

    # pull parent station data for joining to events dataframe
    parent_station_query = sa.select(
        StaticStops.timestamp.label("fk_static_timestamp"),
        StaticStops.stop_id,
        StaticStops.parent_station,
    ).where(StaticStops.timestamp.in_(static_timestamps))
    parent_stations = db_manager.select_as_dataframe(parent_station_query)

    # join parent stations to events on "stop_id" and gtfs static
    # timestamp foreign key
    events_dataframe = events_dataframe.merge(
        parent_stations, how="left", on=["fk_static_timestamp", "stop_id"]
    )
    # is parent station is not provided, transfer "stop_id" value to
    # "parent_station" column
    events_dataframe["parent_station"] = numpy.where(
        events_dataframe["parent_station"].isna(),
        events_dataframe["stop_id"],
        events_dataframe["parent_station"],
    )

    process_logger.log_complete()

    return events_dataframe


def remove_bus_records(
    events_dataframe: pandas.DataFrame,
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    remove all records from dataframe associated with bus trips

    events_dataframe must have "fk_static_timestamp" and "route_id" columns

    route_type == 3 from the routes table indicates bus service, drop all records
    assocated with route_type == 3
    """
    process_logger = ProcessLogger(
        "gtfs_rt.remove_bus_records",
        start_row_count=events_dataframe.shape[0],
    )
    process_logger.log_start()

    # unique list of "fk_static_timestamp" values for pulling route info
    static_timestamps = [
        int(timestamp)
        for timestamp in events_dataframe["fk_static_timestamp"].unique()
    ]

    # pull non-bus route_id's from RDS
    non_bus_id_query = sa.select(
        StaticRoutes.timestamp.label("fk_static_timestamp"),
        StaticRoutes.route_id,
    ).where(
        StaticRoutes.timestamp.in_(static_timestamps),
        StaticRoutes.route_type != 3,
    )
    non_bus_ids = db_manager.select_as_dataframe(non_bus_id_query)

    # join events on non-bus "route_id"s and gtfs static timestamp foreign key
    events_dataframe = events_dataframe.merge(
        non_bus_ids, how="inner", on=["fk_static_timestamp", "route_id"]
    )

    process_logger.add_metadata(after_row_count=events_dataframe.shape[0])
    process_logger.log_complete()

    return events_dataframe
