from typing import List, Union, Dict, Tuple

import numpy
import pandas
import pyarrow.compute as pc
from lamp_py.aws.s3 import read_parquet
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import (
    add_parent_station_column,
    add_missing_service_dates,
    add_static_version_key_column,
    rail_routes_from_filepath,
    start_time_to_seconds,
    unique_trip_stop_columns,
)


def get_vp_dataframe(to_load: Union[str, List[str]], route_ids: List[str]) -> pandas.DataFrame:
    """
    return a dataframe from a vehicle position parquet file (or list of files)
    with expected columns without null data.
    """
    process_logger = ProcessLogger("vp.get_dataframe")
    process_logger.log_start()

    vehicle_position_cols = [
        "vehicle.current_status",
        "vehicle.current_stop_sequence",
        "vehicle.stop_id",
        "vehicle.timestamp",
        "vehicle.trip.direction_id",
        "vehicle.trip.route_id",
        "vehicle.trip.start_date",
        "vehicle.trip.start_time",
        "vehicle.trip.revenue",
        "vehicle.vehicle.id",
        "vehicle.trip.trip_id",
        "vehicle.vehicle.label",
        "vehicle.vehicle.consist",
        "vehicle.multi_carriage_details",
    ]

    vehicle_position_filters = (
        (pc.field("vehicle.current_status").is_valid())
        & (pc.field("vehicle.current_stop_sequence") >= 0)
        & (pc.field("vehicle.stop_id").is_valid())
        & (pc.field("vehicle.timestamp") > 0)
        & (pc.field("vehicle.trip.direction_id").isin((0, 1)))
        & (pc.field("vehicle.trip.route_id").is_valid())
        & (pc.field("vehicle.vehicle.id").is_valid())
        & (pc.field("vehicle.trip.route_id").isin(route_ids))
        & (pc.field("vehicle.trip.trip_id").is_valid())
    )

    rename_mapper = {
        "vehicle.current_status": "current_status",
        "vehicle.current_stop_sequence": "current_stop_sequence",
        "vehicle.stop_id": "stop_id",
        "vehicle.timestamp": "vehicle_timestamp",
        "vehicle.trip.direction_id": "direction_id",
        "vehicle.trip.route_id": "route_id",
        "vehicle.trip.start_date": "start_date",
        "vehicle.trip.start_time": "start_time",
        "vehicle.trip.revenue": "revenue",
        "vehicle.vehicle.id": "vehicle_id",
        "vehicle.trip.trip_id": "trip_id",
        "vehicle.vehicle.label": "vehicle_label",
        "vehicle.vehicle.consist": "vehicle_consist",
        "vehicle.multi_carriage_details": "multi_carriage_details",
    }

    result = read_parquet(
        to_load,
        columns=vehicle_position_cols,
        filters=vehicle_position_filters,
    )

    result = result.rename(columns=rename_mapper)

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
    process_logger = ProcessLogger("vp.transform_datatypes", row_count=vehicle_positions.shape[0])
    process_logger.log_start()

    # current_status: 1 = MOVING, 0 = STOPPED_AT
    vehicle_positions["is_moving"] = numpy.where(
        vehicle_positions["current_status"] != "STOPPED_AT", True, False
    ).astype(numpy.bool_)
    vehicle_positions = vehicle_positions.drop(columns=["current_status"])

    # rename start_date to service date and store as int64 instead of string
    vehicle_positions.rename(columns={"start_date": "service_date"}, inplace=True)
    vehicle_positions["service_date"] = pandas.to_numeric(vehicle_positions["service_date"]).astype("Int64")

    # rename current_stop_sequence to stop_sequence
    # and convert to int64
    vehicle_positions.rename(columns={"current_stop_sequence": "stop_sequence"}, inplace=True)
    vehicle_positions["stop_sequence"] = pandas.to_numeric(vehicle_positions["stop_sequence"]).astype("int64")

    # store direction_id as bool
    vehicle_positions["direction_id"] = pandas.to_numeric(vehicle_positions["direction_id"]).astype(numpy.bool_)

    # fix revenue field, NULL is True
    vehicle_positions["revenue"] = numpy.where(vehicle_positions["revenue"].eq(False), False, True).astype(numpy.bool_)

    # store start_time as seconds from start of day as int64
    vehicle_positions["start_time"] = vehicle_positions["start_time"].apply(start_time_to_seconds).astype("Int64")

    process_logger.log_complete()
    return vehicle_positions


def transform_vp_timestamps(
    vehicle_positions: pandas.DataFrame,
) -> pandas.DataFrame:
    """
    convert raw vp data into a timestamped event data for each stop on a trip.

    this method will add
    * "vp_move_timestamp" - when the vehicle begins moving towards the event parent_staion
    * "vp_stop_timestamp" - when the vehicle arrives at the event parent_station

    this method will remove "is_moving" and "vehicle_timestamp"
    """
    process_logger = ProcessLogger("vp.transform_timestamps", start_row_count=vehicle_positions.shape[0])
    process_logger.log_start()

    trip_stop_columns = unique_trip_stop_columns()

    # create a pivot table on unique trip-stop events, finding the earliest time
    # that each vehicle/stop pair is and is not moving. rename the vehicle
    # timestamps to vp_stop_timestamp and vp_move_timestamp, the names used
    # in the database
    vp_timestamps = pandas.pivot_table(
        vehicle_positions,
        index=trip_stop_columns,
        columns="is_moving",
        aggfunc={"vehicle_timestamp": "min"},
    ).reset_index(drop=False)

    rename_mapper: Dict[Tuple[str, Union[str, bool]], str] = {(column, ""): column for column in trip_stop_columns}
    rename_mapper.update({("vehicle_timestamp", True): "vp_move_timestamp"})
    rename_mapper.update({("vehicle_timestamp", False): "vp_stop_timestamp"})

    vp_timestamps.columns = vp_timestamps.columns.to_flat_index()
    vp_timestamps = vp_timestamps.rename(columns=rename_mapper)
    # verify timestamp columns were created
    for column in ("vp_stop_timestamp", "vp_move_timestamp"):
        if column not in vp_timestamps.columns:
            vp_timestamps[column] = None

    # we no longer need is moving or vehicle timestamp as those are all
    # stored in the vp_timestamps dataframe. drop duplicated trip-stop events
    vehicle_positions = vehicle_positions.drop(columns=["is_moving", "vehicle_timestamp"]).drop_duplicates(
        subset=trip_stop_columns
    )

    # join the timestamps to trip-stop details, leaving us with vp move and
    # stop times
    vehicle_positions = pandas.merge(
        vp_timestamps,
        vehicle_positions,
        how="left",
        on=trip_stop_columns,
        validate="one_to_one",
    )

    vehicle_positions["vp_move_timestamp"] = vehicle_positions["vp_move_timestamp"].astype("Int64")
    vehicle_positions["vp_stop_timestamp"] = vehicle_positions["vp_stop_timestamp"].astype("Int64")

    # change vehicle_consist to pipe delimited string
    vehicle_positions["vehicle_consist"] = vehicle_positions["vehicle_consist"].map(
        lambda vc: "|".join(str(vc_val["label"]) for vc_val in vc),
        na_action="ignore",
    )

    # change multi_carriage_details to pipe delimited string
    vehicle_positions["multi_carriage_details"] = vehicle_positions["multi_carriage_details"].map(
        lambda vc: "|".join(str(vc_val["label"]) for vc_val in vc),
        na_action="ignore",
    )

    # coalesce vehicle_consist with multi_carriage_details.
    # vehicle_consist dropped from RT_VEHICLE_POSITIONS feed on 2024-03-05
    vehicle_positions["vehicle_consist"] = numpy.where(
        vehicle_positions["vehicle_consist"].isnull(),
        vehicle_positions["multi_carriage_details"],
        vehicle_positions["vehicle_consist"],
    )
    vehicle_positions = vehicle_positions.drop(columns=["multi_carriage_details"])

    process_logger.add_metadata(after_row_count=vehicle_positions.shape[0])
    process_logger.log_complete()
    return vehicle_positions


def process_vp_files(
    paths: Union[str, List[str]],
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    Generate a dataframe of Vehicle Events from gtfs_rt vehicle position parquet files.
    """
    process_logger = ProcessLogger("process_vehicle_positions", file_count=len(paths), paths=paths)
    process_logger.log_start()

    route_ids = rail_routes_from_filepath(paths, db_manager)
    vehicle_positions = get_vp_dataframe(paths, route_ids)
    if vehicle_positions.shape[0] > 0:
        vehicle_positions = transform_vp_datatypes(vehicle_positions)
        vehicle_positions = add_missing_service_dates(vehicle_positions, timestamp_key="vehicle_timestamp")
        vehicle_positions = add_static_version_key_column(vehicle_positions, db_manager)
        vehicle_positions = add_parent_station_column(vehicle_positions, db_manager)
        vehicle_positions = transform_vp_timestamps(vehicle_positions)

    process_logger.add_metadata(vehicle_events_count=vehicle_positions.shape[0])
    process_logger.log_complete()
    return vehicle_positions
