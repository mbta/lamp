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
    with expected columns without null data.
    """
    process_logger = ProcessLogger("vp.get_dataframe")
    process_logger.log_start()

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

    result = read_parquet(
        to_load, columns=vehicle_position_cols, filters=vehicle_position_filters
    )

    process_logger.add_metadata(row_count=result.shape[0])
    process_logger.log_complete()

    return result


def transform_vp_datatypes(
    vehicle_positions: pandas.DataFrame,
) -> pandas.DataFrame:
    """
    ingest dataframe of vehicle position data from parquet file and transform
    column datatypes
    """
    process_logger = ProcessLogger(
        "vp.transform_datatypes", row_count=vehicle_positions.shape[0]
    )
    process_logger.log_start()

    # current_staus: 1 = MOVING, 0 = STOPPED_AT
    vehicle_positions["is_moving"] = numpy.where(
        vehicle_positions["current_status"] != "STOPPED_AT", True, False
    ).astype(numpy.bool8)
    vehicle_positions = vehicle_positions.drop(columns=["current_status"])

    # store start_date as int64 instead of string
    vehicle_positions["start_date"] = pandas.to_numeric(
        vehicle_positions["start_date"]
    ).astype("int64")

    # rename current_stop_sequence as stop_sequence
    # and convert to int64
    vehicle_positions.rename(
        columns={"current_stop_sequence": "stop_sequence"}, inplace=True
    )
    vehicle_positions["stop_sequence"] = pandas.to_numeric(
        vehicle_positions["stop_sequence"]
    ).astype("int64")

    # store direction_id as bool
    vehicle_positions["direction_id"] = pandas.to_numeric(
        vehicle_positions["direction_id"]
    ).astype(numpy.bool8)

    # store start_time as seconds from start of day as int64
    vehicle_positions["start_time"] = (
        vehicle_positions["start_time"]
        .apply(start_time_to_seconds)
        .astype("int64")
    )

    process_logger.log_complete()
    return vehicle_positions


def join_vp_with_gtfs_static(
    vehicle_positions: pandas.DataFrame, db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    join vehicle position dataframe to gtfs static records

    adds "fk_static_timestamp" and "parent_station" columns to dataframe
    """
    process_logger = ProcessLogger(
        "vp.join_with_gtfs_static", row_count=vehicle_positions.shape[0]
    )
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

    this method will remove "is_moving" and "vehicle_timestamp"
    """
    process_logger = ProcessLogger(
        "vp.transform_timestamps", start_row_count=vehicle_positions.shape[0]
    )
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

    # create a pivot table on the trip stop hash, finding the earliest time
    # that each vehicle/stop pair is and is not moving. rename the vehicle
    # timestamps to vp_stop_timestamp and vp_move_timestamp, the names used
    # in the database
    vp_timestamps = pandas.pivot_table(
        vehicle_positions,
        index="trip_stop_hash",
        columns="is_moving",
        aggfunc={"vehicle_timestamp": min},
    ).reset_index(drop=False)

    vp_timestamps.columns = vp_timestamps.columns.to_flat_index()
    vp_timestamps = vp_timestamps.rename(
        columns={
            ("trip_stop_hash", ""): "trip_stop_hash",
            ("vehicle_timestamp", False): "vp_stop_timestamp",
            ("vehicle_timestamp", True): "vp_move_timestamp",
        }
    )

    # we no longer need is moving or vehicle timestamp as those are all
    # stored in the vp_timestamps dataframe. drop duplicated trip stop hash
    # rows. this df is now a hash map back to the vehicle and stop data.
    vehicle_positions = vehicle_positions.drop(
        columns=["is_moving", "vehicle_timestamp"]
    ).drop_duplicates(subset="trip_stop_hash")

    # join the timestamps to the hash map, leaving us with vp move and
    # stop times attached to trip stop hashes and the data the went into
    # making them.
    vehicle_positions = pandas.merge(
        vp_timestamps,
        vehicle_positions,
        how="left",
        on="trip_stop_hash",
        validate="one_to_one",
    )

    vehicle_positions["vp_move_timestamp"] = vehicle_positions[
        "vp_move_timestamp"
    ].astype("Int64")
    vehicle_positions["vp_stop_timestamp"] = vehicle_positions[
        "vp_stop_timestamp"
    ].astype("Int64")

    process_logger.add_metadata(after_row_count=vehicle_positions.shape[0])
    process_logger.log_complete()
    return vehicle_positions


def process_vp_files(
    paths: Union[str, List[str]], db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    Generate a dataframe of Vehicle Events froom gtfs_rt vehicle position parquet files.
    """
    process_logger = ProcessLogger(
        "process_vehicle_positions", file_count=len(paths)
    )
    process_logger.log_start()

    vehicle_positions = get_vp_dataframe(paths)
    vehicle_positions = transform_vp_datatypes(vehicle_positions)
    vehicle_positions = join_vp_with_gtfs_static(
        vehicle_positions=vehicle_positions, db_manager=db_manager
    )
    vehicle_positions = transform_vp_timestamps(vehicle_positions)

    process_logger.add_metadata(vehicle_events_count=vehicle_positions.shape[0])
    process_logger.log_complete()
    return vehicle_positions
