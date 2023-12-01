import datetime
from typing import Optional, List, Union

import numpy
import pandas
import pytz
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.rail_performance_manager_schema import (
    StaticFeedInfo,
    StaticRoutes,
    StaticStops,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import get_datetime_from_partition_path


# boston tzinfo to be used with datetimes that require DST considerations.
#
# NOTE: do not use this as an argument for datetime.deatime constructors.
# instead use BOSTON_TZ with a naive datetime. (https://pypi.org/project/pytz/)
BOSTON_TZ = pytz.timezone("EST5EDT")


def start_time_to_seconds(
    time: Optional[str],
) -> Optional[int]:
    """
    transform time string in HH:MM:SS format to seconds
    """
    if time is None:
        return time

    try:
        (hour, minute, second) = time.split(":")
        return int(hour) * 3600 + int(minute) * 60 + int(second)
    except ValueError:
        # some older files have the start time already formatted as seconds
        # after midnight. in those cases, convert to an int and pass through.
        return int(time)


def start_timestamp_to_seconds(start_timestamp: int) -> int:
    """
    convert a start timestamp into seconds after midnight of its service date.
    """
    service_date_string = str(service_date_from_timestamp(start_timestamp))
    year = int(service_date_string[:4])
    month = int(service_date_string[4:6])
    day = int(service_date_string[6:8])
    start_of_service_day = int(
        BOSTON_TZ.localize(
            datetime.datetime(year=year, month=month, day=day)
        ).timestamp()
    )

    return start_timestamp - start_of_service_day


def service_date_from_timestamp(timestamp: int) -> int:
    """
    generate the service date from a timestamp. if the timestamp is from before
    3am, it belongs to the previous days service. otherwise, it belongs to its
    days service. use the EST timezone when interpreting the timestamp to ensure
    proper handling of daylight savings time.
    """
    date_and_time = datetime.datetime.fromtimestamp(timestamp, tz=BOSTON_TZ)

    if date_and_time.hour < 3:
        service_date = date_and_time.date() - datetime.timedelta(days=1)
    else:
        service_date = date_and_time.date()

    return int(
        f"{service_date.year:04}{service_date.month:02}{service_date.day:02}"
    )


def add_missing_service_dates(
    events_dataframe: pandas.DataFrame, timestamp_key: str
) -> pandas.DataFrame:
    """
    # generate the service date from the vehicle timestamp if null
    """
    events_dataframe["service_date"] = events_dataframe["service_date"].where(
        events_dataframe["service_date"].notna(),
        events_dataframe[timestamp_key].apply(service_date_from_timestamp),
    )

    return events_dataframe


def unique_trip_stop_columns() -> List[str]:
    """
    columns used to determine if a event is a unique trip stop
    """
    return [
        "service_date",
        "route_id",
        "trip_id",
        "parent_station",
    ]


def static_version_key_from_service_date(
    service_date: int, db_manager: DatabaseManager
) -> int:
    """
    for a given service date, determine the correct static schedule to use
    """
    # the service date must:
    # * be between "feed_start_date" and "feed_end_date" in StaticFeedInfo
    # * be less than or equal to "feed_active_date" in StaticFeedInfo
    #
    # order all static version keys by feed_active_date descending and
    # created_on date descending, then choose the first tone. this handles
    # multiple static schedules being issued for the same service day
    live_match_query = (
        sa.select(StaticFeedInfo.static_version_key)
        .where(
            StaticFeedInfo.feed_start_date <= service_date,
            StaticFeedInfo.feed_end_date >= service_date,
            StaticFeedInfo.feed_active_date <= service_date,
        )
        .order_by(
            StaticFeedInfo.feed_active_date.desc(),
            StaticFeedInfo.created_on.desc(),
        )
        .limit(1)
    )

    # "feed_start_date" and "feed_end_date" are modified for archived GTFS
    # Schedule files. If processing archived static schedules, these alternate
    # rules must be used for matching GTFS static to GTFS-RT data
    archive_match_query = (
        sa.select(StaticFeedInfo.static_version_key)
        .where(
            StaticFeedInfo.feed_start_date <= service_date,
            StaticFeedInfo.feed_end_date >= service_date,
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
            f"StaticFeedInfo table has no matching schedule for service_date={service_date}"
        )

    return int(result[0]["static_version_key"])


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
        service_date = int(date)
        static_version_key = static_version_key_from_service_date(
            service_date=service_date, db_manager=db_manager
        )

        service_date_mask = events_dataframe["service_date"] == service_date
        events_dataframe.loc[
            service_date_mask, "static_version_key"
        ] = static_version_key

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


def rail_routes_from_filepath(
    filepath: Union[List[str], str], db_manager: DatabaseManager
) -> List[str]:
    """
    get a list of rail route_ids that were in effect on a given service date
    described by a timestamp. the schedule version is derived from the service
    date. poll that version of the schedule for all route ids whos route type
    is not 3 (a bus route).
    """
    if isinstance(filepath, list):
        filepath = filepath[0]

    date = get_datetime_from_partition_path(filepath)
    service_date = int(f"{date.year:04}{date.month:02}{date.day:02}")

    static_version_key = static_version_key_from_service_date(
        service_date=service_date, db_manager=db_manager
    )

    result = db_manager.execute(
        sa.select(StaticRoutes.route_id).where(
            StaticRoutes.route_type.in_([0, 1, 2]),
            StaticRoutes.static_version_key == static_version_key,
        )
    )

    return [row[0] for row in result]
