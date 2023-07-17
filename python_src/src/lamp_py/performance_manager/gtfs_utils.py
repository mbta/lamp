from typing import Optional, List

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


def unique_trip_stop_columns() -> List[str]:
    """
    columns used to determine if a event is a unique trip stop
    """
    return [
        "service_date",
        "start_time",
        "route_id",
        "direction_id",
        "vehicle_id",
        "parent_station",
    ]


def add_static_version_key_column(
    events_dataframe: pandas.DataFrame,
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    adds "static_version_key" column to dataframe

    using "static_version_key" column, events dataframe records may be joined to
    gtfs static record tables
    """
    # based on discussions with OPMI, matching of GTFS-RT events to GTFS-static schedule versions
    # will occur on a whole 'service_date' basis
    #
    # when processing live GTFS-static schedule versions, matching can only apply to, at the earliest,
    # the current `service_date` when processed, no retroactive assignment to past days will occur.
    #
    # extraction of `feed_active_date` from `feed_version` of the GTFS-static FEED_INFO table
    # is currently handled by an DB Trigger function added by alembic migration Revision ID: 43153d536c2a

    process_logger = ProcessLogger(
        "add_static_version_key",
        row_count=events_dataframe.shape[0],
    )
    process_logger.log_start()

    # initialize static_version_key column
    events_dataframe["static_version_key"] = 0

    for date in events_dataframe["service_date"].unique():
        date = int(date)
        # "service_date" from events dataframe must be between "feed_start_date" and "feed_end_date" in StaticFeedInfo
        # "service_date" must also be less than or equal to "feed_active_date" in StaticFeedInfo
        # StaticFeedInfo, order by feed_active_date descending and created_on date descending
        # this should deal with multiple static schedules being issued on the same day
        # if this occurs we will use the latest issued schedule
        live_match_query = (
            sa.select(StaticFeedInfo.static_version_key)
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
            sa.select(StaticFeedInfo.static_version_key)
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
                f"StaticFeedInfo table has no matching schedule for service_date={date}"
            )

        service_date_mask = events_dataframe["service_date"] == date
        events_dataframe.loc[service_date_mask, "static_version_key"] = int(
            result[0]["static_version_key"]
        )

    process_logger.log_complete()

    return events_dataframe


def add_parent_station_column(
    events_dataframe: pandas.DataFrame,
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    adds "parent_station" column to dataframe

    events_dataframe must have "static_version_key" and "stop_id" columns

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

    # unique list of "static_version_key" values for pulling parent stations
    lookup_v_keys = [
        int(s_v_key)
        for s_v_key in events_dataframe["static_version_key"].unique()
    ]

    # pull parent station data for joining to events dataframe
    parent_station_query = sa.select(
        StaticStops.static_version_key,
        StaticStops.stop_id,
        StaticStops.parent_station,
    ).where(StaticStops.static_version_key.in_(lookup_v_keys))
    parent_stations = db_manager.select_as_dataframe(parent_station_query)

    # join parent stations to events on "stop_id" and "static_version_key" foreign key
    events_dataframe = events_dataframe.merge(
        parent_stations, how="left", on=["static_version_key", "stop_id"]
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

    events_dataframe must have "static_version_key" and "route_id" columns

    route_type == 3 from the routes table indicates bus service, drop all records
    assocated with route_type == 3
    """
    process_logger = ProcessLogger(
        "gtfs_rt.remove_bus_records",
        start_row_count=events_dataframe.shape[0],
    )
    process_logger.log_start()

    # unique list of "static_version_key" values for pulling route info
    lookup_v_keys = [
        int(s_v_key)
        for s_v_key in events_dataframe["static_version_key"].unique()
    ]

    # pull non-bus route_id's from RDS
    non_bus_id_query = sa.select(
        StaticRoutes.static_version_key,
        StaticRoutes.route_id,
    ).where(
        StaticRoutes.static_version_key.in_(lookup_v_keys),
        StaticRoutes.route_type != 3,
    )
    non_bus_ids = db_manager.select_as_dataframe(non_bus_id_query)

    # join events on non-bus "route_id"s and "static_version_key" foreign key
    events_dataframe = events_dataframe.merge(
        non_bus_ids, how="inner", on=["static_version_key", "route_id"]
    )

    process_logger.add_metadata(after_row_count=events_dataframe.shape[0])
    process_logger.log_complete()

    return events_dataframe
