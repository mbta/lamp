from typing import Any, Dict, Iterator, List, Optional, Union

import numpy
import pandas
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
        "timestamp",
        "stop_time_update",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
        "trip_id",
    ]
    trip_update_filters = [
        ("direction_id", "in", (0, 1)),
        ("timestamp", ">", 0),
        ("route_id", "!=", "None"),
        ("trip_id", "!=", "None"),
        ("vehicle_id", "!=", "None"),
        ("route_id", "in", route_ids),
    ]
    # 100_000 batch size should result in ~5-6 GB of memory use per batch
    # of trip update records
    return read_parquet_chunks(
        to_load,
        max_rows=100_000,
        columns=trip_update_columns,
        filters=trip_update_filters,
    )


# pylint: disable=too-many-arguments
def explode_stop_time_update(
    stop_time_update: Optional[List[Dict[str, Any]]],
    timestamp: int,
    direction_id: bool,
    route_id: Any,
    service_date: Optional[int],
    start_time: Optional[int],
    vehicle_id: Any,
    trip_id: Any,
) -> Optional[List[dict]]:
    """
    explode nested list of dicts in stop_time_update column

    to be used with numpy vectorize
    """
    append_dict = {
        "timestamp": timestamp,
        "direction_id": direction_id,
        "route_id": route_id,
        "service_date": service_date,
        "start_time": start_time,
        "vehicle_id": vehicle_id,
        "trip_id": trip_id,
    }
    return_list: List[Dict[str, Any]] = []

    # fix: https://app.asana.com/0/1203185331040541/1203495730837934
    # it appears that numpy.vectorize batches function inputs which can
    # result in None being passed in for stop_time_update
    if stop_time_update is None:
        return None

    for record in stop_time_update:
        try:
            arrival_time = int(record["arrival"]["time"])
        except (TypeError, KeyError):
            continue
        # filter out stop event predictions that are too far into the future
        # and are unlikely to be used as a final stop event prediction
        # (2 minutes) or predictions that go into the past (negative values)
        if arrival_time - timestamp < 0 or arrival_time - timestamp > 120:
            continue
        append_dict.update(
            {
                "stop_id": record.get("stop_id"),
                "tu_stop_timestamp": arrival_time,
            }
        )
        return_list.append(append_dict.copy())

    if len(return_list) == 0:
        return None

    return return_list


# pylint: enable=too-many-arguments


def get_and_unwrap_tu_dataframe(
    paths: Union[str, List[str]], route_ids: List[str]
) -> pandas.DataFrame:
    """
    unwrap and explode trip updates records from parquet files
    parquet files contain stop_time_update field that is saved as list of dicts
    stop_time_update must have fields extracted and flattened to create
    predicted trip update stop events
    """
    process_logger = ProcessLogger("tu.get_and_unwrap_dataframe")
    process_logger.log_start()

    events = pandas.Series(dtype="object")

    # get_tu_dataframe_chunks set to pull ~100_000 trip update records
    # per batch, this should result in ~5-6 GB of memory use per batch
    # after batch goes through explod_stop_time_update vectorize operation,
    # resulting Series has negligible memory use
    for batch_events in get_tu_dataframe_chunks(paths, route_ids):
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

        # expand and filter stop_time_update column using numpy vectorize
        # numpy vectorize offers significantly better performance over pandas apply
        # this will return a ndarray with values being list of dicts
        vector_explode = numpy.vectorize(explode_stop_time_update)
        batch_events = pandas.Series(
            vector_explode(
                batch_events.stop_time_update,
                batch_events.timestamp,
                batch_events.direction_id,
                batch_events.route_id,
                batch_events.service_date,
                batch_events.start_time,
                batch_events.vehicle_id,
                batch_events.trip_id,
            )
        ).dropna()
        events = pandas.concat([events, batch_events])

    # transform Series of list of dicts into dataframe
    trip_updates = pandas.json_normalize(events.explode())

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
        "process_trip_updates", file_count=len(paths)
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
