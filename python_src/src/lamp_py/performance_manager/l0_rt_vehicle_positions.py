from typing import List, Union

import numpy
import pandas
from lamp_py.aws.s3 import read_parquet
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import (
    add_event_hash_column,
    start_time_to_seconds,
    add_fk_static_timestamp_column,
    add_parent_station_column,
    remove_bus_records,
)


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
    ).astype(numpy.bool_)
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
    ).astype(numpy.bool_)

    # store start_time as seconds from start of day as int64
    vehicle_positions["start_time"] = (
        vehicle_positions["start_time"]
        .apply(start_time_to_seconds)
        .astype("int64")
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
    # verify timestamp columns were created
    for column in ("vp_stop_timestamp", "vp_move_timestamp"):
        if column not in vp_timestamps.columns:
            vp_timestamps[column] = None

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
    vehicle_positions = add_fk_static_timestamp_column(
        vehicle_positions, "vehicle_timestamp", db_manager
    )
    vehicle_positions = remove_bus_records(vehicle_positions, db_manager)
    vehicle_positions = add_parent_station_column(vehicle_positions, db_manager)
    vehicle_positions = transform_vp_timestamps(vehicle_positions)

    process_logger.add_metadata(vehicle_events_count=vehicle_positions.shape[0])
    process_logger.log_complete()
    return vehicle_positions
