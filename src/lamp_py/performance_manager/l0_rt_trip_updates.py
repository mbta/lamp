from typing import Iterator, List, Union
import time

import numpy
import pandas
import pyarrow.compute as pc
from lamp_py.aws.s3 import read_parquet_chunks
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import (
    add_missing_service_dates,
    add_parent_station_column,
    add_static_version_key_column,
    rail_routes_from_filepath,
    start_time_to_seconds,
    unique_trip_stop_columns,
)


def get_tu_dataframe_chunks(
    to_load: Union[str, List[str]], route_ids: List[str]
) -> Iterator[pandas.DataFrame]:
    """
    return interator of dataframe chunks from a trip updates parquet file
    (or list of files)
    """
    trip_update_columns = [
        "feed_timestamp",
        "trip_update.timestamp",
        "trip_update.stop_time_update.stop_id",
        "trip_update.stop_time_update.arrival.time",
        "trip_update.trip.direction_id",
        "trip_update.trip.route_id",
        "trip_update.trip.start_date",
        "trip_update.trip.start_time",
        "trip_update.vehicle.id",
        "trip_update.trip.trip_id",
    ]
    trip_update_filters = (
        (pc.field("trip_update.trip.direction_id").isin((0, 1)))
        & (pc.field("trip_update.trip.trip_id").is_valid())
        & (pc.field("trip_update.vehicle.id").is_valid())
        & (pc.field("trip_update.trip.route_id").isin(route_ids))
        & (pc.field("trip_update.stop_time_update.arrival.time") > 0)
    )

    # 100_000 batch size should result in ~5-6 GB of memory use per batch
    # of trip update records
    return read_parquet_chunks(
        to_load,
        max_rows=1_000_000,
        columns=trip_update_columns,
        filters=trip_update_filters,
    )


def get_and_unwrap_tu_dataframe(
    paths: Union[str, List[str]], route_ids: List[str]
) -> pandas.DataFrame:
    """
    get trip updates records from parquet files
    to create predicted trip update stop events
    """
    process_logger = ProcessLogger("tu.get_and_unwrap_dataframe")
    process_logger.log_start()

    trip_updates = pandas.DataFrame()

    rename_mapper = {
        "trip_update.timestamp": "timestamp",
        "trip_update.stop_time_update.stop_id": "stop_id",
        "trip_update.stop_time_update.arrival.time": "tu_stop_timestamp",
        "trip_update.trip.direction_id": "direction_id",
        "trip_update.trip.route_id": "route_id",
        "trip_update.trip.start_date": "start_date",
        "trip_update.trip.start_time": "start_time",
        "trip_update.vehicle.id": "vehicle_id",
        "trip_update.trip.trip_id": "trip_id",
    }

    retry_attempts = 2
    for retry_attempt in range(retry_attempts + 1):
        try:
            process_logger.add_metadata(retry_attempts=retry_attempt)
            for batch_events in get_tu_dataframe_chunks(paths, route_ids):
                # rename columns from trip update parquet schema
                batch_events = batch_events.rename(columns=rename_mapper)
                # use feed_timestamp if timestamp value is null
                batch_events["timestamp"] = batch_events["timestamp"].where(
                    batch_events["timestamp"].notna(),
                    batch_events["feed_timestamp"],
                )
                batch_events = batch_events.drop(columns=["feed_timestamp"])

                # store start_date as int64 and rename to service_date
                batch_events.rename(
                    columns={"start_date": "service_date"}, inplace=True
                )
                batch_events["service_date"] = pandas.to_numeric(
                    batch_events["service_date"]
                ).astype("Int64")

                # store direction_id as bool
                batch_events["direction_id"] = pandas.to_numeric(
                    batch_events["direction_id"]
                ).astype(numpy.bool_)

                # store start_time as seconds from start of day int64
                batch_events["start_time"] = (
                    batch_events["start_time"]
                    .apply(start_time_to_seconds)
                    .astype("Int64")
                )

                batch_events["tu_stop_timestamp"] = pandas.to_numeric(
                    batch_events["tu_stop_timestamp"]
                ).astype("Int64")

                # filter out stop event predictions that are too far into the future
                # and are unlikely to be used as a final stop event prediction
                # (2 minutes) or predictions that go into the past (negative values)
                batch_events = batch_events[
                    (
                        batch_events["tu_stop_timestamp"]
                        - batch_events["timestamp"]
                        >= 0
                    )
                    & (
                        batch_events["tu_stop_timestamp"]
                        - batch_events["timestamp"]
                        < 120
                    )
                ]

                trip_updates = pandas.concat([trip_updates, batch_events])
            break
        except Exception as exception:
            if retry_attempt == retry_attempts:
                process_logger.log_failure(exception)
                raise exception
            time.sleep(1)

    process_logger.add_metadata(row_count=trip_updates.shape[0])
    process_logger.log_complete()

    return trip_updates


def reduce_trip_updates(trip_updates: pandas.DataFrame) -> pandas.DataFrame:
    """
    reduce the data frame to a single record per trip / stop.
    """
    process_logger = ProcessLogger(
        "tu.reduce", start_row_count=trip_updates.shape[0]
    )
    process_logger.log_start()

    trip_stop_columns = unique_trip_stop_columns()

    # sort all trip updates by reverse timestamp, then drop all of the updates
    # for the same trip and same station but the first one. the first update will
    # be the most recent arrival time prediction
    trip_updates = trip_updates.sort_values(by=["timestamp"], ascending=False)
    trip_updates = trip_updates.drop_duplicates(
        subset=trip_stop_columns, keep="first"
    )

    # after group and sort, "timestamp" longer needed
    trip_updates = trip_updates.drop(columns=["timestamp"])

    trip_updates["tu_stop_timestamp"] = trip_updates[
        "tu_stop_timestamp"
    ].astype("Int64")

    # add selected columns
    # trip_updates and vehicle_positions dataframes must all have the same columns
    # available to be correctly joined in combine_events function of l0_gtfs_rt_events.py
    trip_updates["stop_sequence"] = None
    trip_updates["vehicle_label"] = None
    trip_updates["vehicle_consist"] = None

    process_logger.add_metadata(after_row_count=trip_updates.shape[0])
    process_logger.log_complete()

    return trip_updates


def process_tu_files(
    paths: Union[str, List[str]],
    db_manager: DatabaseManager,
) -> pandas.DataFrame:
    """
    Generate a dataframe of Vehicle Events from gtfs_rt trip updates parquet files.
    """
    process_logger = ProcessLogger(
        "process_trip_updates", file_count=len(paths), paths=paths
    )
    process_logger.log_start()

    route_ids = rail_routes_from_filepath(paths, db_manager)
    trip_updates = get_and_unwrap_tu_dataframe(paths, route_ids)
    if trip_updates.shape[0] > 0:
        trip_updates = add_missing_service_dates(
            events_dataframe=trip_updates, timestamp_key="timestamp"
        )
        trip_updates = add_static_version_key_column(trip_updates, db_manager)
        trip_updates = add_parent_station_column(trip_updates, db_manager)
        trip_updates = reduce_trip_updates(trip_updates)

    process_logger.add_metadata(vehicle_events_count=trip_updates.shape[0])
    process_logger.log_complete()

    return trip_updates
