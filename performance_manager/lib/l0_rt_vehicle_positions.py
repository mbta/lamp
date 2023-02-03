from typing import List, Union

import numpy
import pandas
import sqlalchemy as sa

from .gtfs_utils import add_event_hash_column, start_time_to_seconds
from .logging_utils import ProcessLogger
from .postgres_schema import StaticFeedInfo, StaticStops
from .postgres_utils import DatabaseManager
from .s3_utils import read_parquet


def get_vp_dataframe(to_load: Union[str, List[str]]) -> pandas.DataFrame:
    """
    return a dataframe from a vehicle position parquet file (or list of files)
    with expected columns
    """
    vehicle_position_cols = [
        "current_status",
        "current_stop_sequence",
        "stop_id",
        "vehicle_timestamp",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]

    vehicle_position_filters = [
        ("current_status", "!=", "None"),
        ("current_stop_sequence", ">=", 0),
        ("stop_id", "!=", "None"),
        ("vehicle_timestamp", ">", 0),
        ("direction_id", "in", (0, 1)),
        ("route_id", "!=", "None"),
        ("start_date", "!=", "None"),
        ("start_time", "!=", "None"),
        ("vehicle_id", "!=", "None"),
    ]

    return read_parquet(
        to_load, columns=vehicle_position_cols, filters=vehicle_position_filters
    )


def transform_vp_dtypes(
    vehicle_positions: pandas.DataFrame,
) -> pandas.DataFrame:
    """
    ingest dataframe of vehicle position data from parquet file and transform
    column datatypes
    """
    # current_staus: 1 = MOVING, 0 = STOPPED_AT
    vehicle_positions["is_moving"] = numpy.where(
        vehicle_positions["current_status"] != "STOPPED_AT", True, False
    ).astype(numpy.bool8)
    vehicle_positions = vehicle_positions.drop(columns=["current_status"])

    # store start_date as Int64 [nullable] instead of string
    vehicle_positions["start_date"] = pandas.to_numeric(
        vehicle_positions["start_date"]
    ).astype("Int64")

    # rename current_stop_sequence as stop_sequence
    # and convert to Int64 [nullable]
    vehicle_positions.rename(
        columns={"current_stop_sequence": "stop_sequence"}, inplace=True
    )
    vehicle_positions["stop_sequence"] = pandas.to_numeric(
        vehicle_positions["stop_sequence"]
    ).astype("Int64")

    # store direction_id as Int64 [nullable]
    vehicle_positions["direction_id"] = pandas.to_numeric(
        vehicle_positions["direction_id"]
    ).astype("Int64")

    # store start_time as seconds from start of day (Int64 [nullable])
    vehicle_positions["start_time"] = (
        vehicle_positions["start_time"]
        .apply(start_time_to_seconds)
        .astype("Int64")
    )

    return vehicle_positions


def join_vp_with_gtfs_static(
    vehicle_positions: pandas.DataFrame, db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    join vehicle position dataframe to gtfs static records

    adds "fk_static_timestamp" and "parent_station" columns to dataframe
    """
    process_logger = ProcessLogger("join_vp_with_gtfs_static")
    process_logger.log_start()

    # get unique "start_date" values from vehicle position dataframe
    # with associated minimum "vehicle_timestamp"
    date_groups = vehicle_positions.groupby(by="start_date")[
        "vehicle_timestamp"
    ].min()

    # dictionary used to match minimum "vehicle_timestamp" values to
    # "timestamp" from StaticFeedInfo table, to be used as foreign key
    timestamp_lookup = {}
    for (date, min_timestamp) in date_groups.iteritems():
        date = int(date)
        min_timestamp = int(min_timestamp)
        # "start_date" from vehicle timestamps must be between "feed_start_date"
        # and "feed_end_date" in StaticFeedInfo
        # minimum "vehicle_timestamp" must also be less than "timestamp" from
        # StaticFeedInfo, order by "timestamp" descending and limit to 1 result
        # this should deal with multiple static schedules with possible
        # overlapping times of applicability
        feed_timestamp_query = (
            sa.select(StaticFeedInfo.timestamp)
            .where(
                (StaticFeedInfo.feed_start_date <= date)
                & (StaticFeedInfo.feed_end_date >= date)
                # & (StaticFeedInfo.timestamp < min_timestamp)
            )
            .order_by(StaticFeedInfo.timestamp.desc())
            .limit(1)
        )
        result = db_manager.select_as_list(feed_timestamp_query)
        # if this query does not produce a result, no static schedule info
        # exists for this vehicle position data, so the vehicle position data
        # should not be processed until valid static schedule data exists
        if len(result) == 0:
            raise IndexError(
                f"StaticFeedInfo table has no matching schedule for start_date={date}, timestamp={min_timestamp}"
            )
        timestamp_lookup[min_timestamp] = int(result[0]["timestamp"])

    vehicle_positions["fk_static_timestamp"] = timestamp_lookup[
        min(timestamp_lookup.keys())
    ]
    # add "fk_static_timestamp" column to vehicle positions dataframe
    # loop is to handle batches vehicle position batches that are applicable to
    # overlapping static gtfs data
    for min_timestamp in sorted(timestamp_lookup.keys()):
        timestamp_mask = vehicle_positions["vehicle_timestamp"] >= min_timestamp
        vehicle_positions.loc[
            timestamp_mask, "fk_static_timestamp"
        ] = timestamp_lookup[min_timestamp]

    # unique list of "fk_static_timestamp" values for pulling parent stations
    static_timestamps = list(set(timestamp_lookup.values()))

    # pull parent station data for joining to vehicle position events
    parent_station_query = sa.select(
        StaticStops.timestamp.label("fk_static_timestamp"),
        StaticStops.stop_id,
        StaticStops.parent_station,
    ).where(StaticStops.timestamp.in_(static_timestamps))
    parent_stations = db_manager.select_as_dataframe(parent_station_query)

    # join parent stations to vehicle positions on "stop_id" and gtfs static
    # timestamp foreign key
    vehicle_positions = vehicle_positions.merge(
        parent_stations, how="left", on=["fk_static_timestamp", "stop_id"]
    )
    # is parent station is not provided, transfer "stop_id" value to
    # "parent_station" column
    vehicle_positions["parent_station"] = numpy.where(
        vehicle_positions["parent_station"].isna(),
        vehicle_positions["stop_id"],
        vehicle_positions["parent_station"],
    )

    process_logger.log_complete()
    return vehicle_positions


def transform_vp_timestamps(
    vehicle_positions: pandas.DataFrame,
) -> pandas.DataFrame:
    """
    convert raw vp data into a timestamped event data for each stop on a trip.

    this method will add
    * "trip_stop_hash" - unique to vehicle/trip/stop
    * "vp_move_timestamp" - when the vehicle begins moving towards the hashed stop
    * "vp_stop_timestamp" - when the vehicle arrives at the hashed stop
    * "pk_id" -

    this method will remove "is_moving" and "vehicle_timestamp"
    """
    process_logger = ProcessLogger("transform_vp_timestamps")
    process_logger.log_start()

    vehicle_positions = add_event_hash_column(
        vehicle_positions,
        hash_column_name="trip_stop_hash",
        expected_hash_columns=[
            "stop_sequence",
            "parent_station",
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
        ],
    )

    vehicle_positions = vehicle_positions.sort_values(
        by=["trip_stop_hash", "is_moving", "vehicle_timestamp"]
    ).drop_duplicates(subset=["trip_stop_hash", "is_moving"], keep="first")

    # get the move timestamp from all events where "is moving" is true
    vehicle_positions["vp_move_timestamp"] = numpy.where(
        vehicle_positions["is_moving"],
        vehicle_positions["vehicle_timestamp"],
        numpy.nan,
    ).astype("int64")

    # get the stop timestamp from all events where "is moving" is false
    vehicle_positions["vp_stop_timestamp"] = numpy.where(
        ~vehicle_positions["is_moving"],
        vehicle_positions["vehicle_timestamp"],
        numpy.nan,
    ).astype("int64")

    # copy all of the move timestamps into rows with stop timestamps
    vehicle_positions["vp_move_timestamp"] = numpy.where(
        (
            (~vehicle_positions["is_moving"])
            & (
                vehicle_positions["trip_stop_hash"]
                == vehicle_positions["trip_stop_hash"].shift(-1)
            )
        ),
        vehicle_positions["vp_move_timestamp"].shift(-1),
        vehicle_positions["vp_move_timestamp"],
    ).astype("int64")

    # for stops that have both moving and stop events, we'll have two rows in
    # the dataframe. the first will have the move and stop timestamps and the
    # second will only have the stop timestsmp. both entries will have the stame
    # stop hash, so remove all duplicates of the hash, keeping the first.
    vehicle_positions = vehicle_positions.drop_duplicates(
        subset=["trip_stop_hash"], keep="first"
    )

    # add a primary key and drop "is_moving" and "vehicle_timestamp"
    vehicle_positions = vehicle_positions.drop(
        columns=["is_moving", "vehicle_timestamp"]
    )
    vehicle_positions.insert(0, "pk_id", numpy.nan)

    process_logger.log_complete()
    return vehicle_positions


def process_vp_files(
    paths: Union[str, List[str]], db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    Generate a dataframe of Vehicle Events froom gtfs_rt vehicle position parquet files.
    """
    process_logger = ProcessLogger("process_vehicle_positions")
    process_logger.log_start()

    vehicle_positions = get_vp_dataframe(paths)
    vehicle_positions = transform_vp_dtypes(vehicle_positions)
    vehicle_positions = join_vp_with_gtfs_static(
        vehicle_positions=vehicle_positions, db_manager=db_manager
    )
    vehicle_positions = transform_vp_timestamps(vehicle_positions)

    process_logger.log_complete()
    return vehicle_positions
