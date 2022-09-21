from typing import Optional, Union, List, Dict, Any

import pandas
import numpy

import sqlalchemy as sa

from .gtfs_utils import start_time_to_seconds, add_event_hash_column
from .logging_utils import ProcessLogger
from .postgres_schema import TripUpdateEvents, MetadataLog
from .postgres_utils import get_unprocessed_files, DatabaseManager
from .s3_utils import read_parquet


def get_tu_dataframe(to_load: Union[str, List[str]]) -> pandas.DataFrame:
    """
    return a dataframe from a trip updates parquet file (or list of files)
    with expected columns
    """
    trip_update_columns = [
        "timestamp",
        "stop_time_update",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]
    trip_update_filters = [
        ("direction_id", "in", (0, 1)),
        ("timestamp", ">", 0),
        ("route_id", "!=", "None"),
        ("start_date", "!=", "None"),
        ("start_time", "!=", "None"),
        ("vehicle_id", "!=", "None"),
    ]

    return read_parquet(
        to_load, columns=trip_update_columns, filters=trip_update_filters
    )


# pylint: disable=too-many-arguments
def explode_stop_time_update(
    stop_time_update: List[Dict[str, Any]],
    timestamp: int,
    direction_id: int,
    route_id: Any,
    start_date: int,
    start_time: int,
    vehicle_id: Any,
) -> Optional[List[dict]]:
    """
    function to be used with numpy vectorize
    explode nested list of dicts in stop_time_update column
    """
    append_dict = {
        "timestamp": timestamp,
        "direction_id": direction_id,
        "route_id": route_id,
        "start_date": start_date,
        "start_time": start_time,
        "vehicle_id": vehicle_id,
    }
    return_list: List[Dict[str, Any]] = []
    for record in stop_time_update:
        try:
            arrival_time = int(record["arrival"]["time"])
        except (TypeError, KeyError):
            continue
        # filter out stop event predictions that are too far into the future
        # and are unlikel to be used as a final stop event prediction
        # (2 minutes) or predictions that go into the past (negative values)
        if arrival_time - timestamp < 0 or arrival_time - timestamp > 120:
            continue
        append_dict.update(
            {
                "stop_id": record.get("stop_id"),
                "stop_sequence": record.get("stop_sequence"),
                "timestamp_start": arrival_time,
            }
        )
        return_list.append(append_dict.copy())

    if len(return_list) == 0:
        return None

    return return_list


# pylint: enable=too-many-arguments


def unwrap_tu_dataframe(events: pandas.DataFrame) -> pandas.DataFrame:
    """
    unwrap and explode trip updates records from parquet files
    parquet files contain stop_time_update field that is saved as list of dicts
    stop_time_update must have fields extracted and flattened to create
    predicted trip update stop events
    """
    # store start_date as int64
    events["start_date"] = pandas.to_numeric(events["start_date"]).astype(
        "int64"
    )

    # store direction_id as int64
    events["direction_id"] = pandas.to_numeric(events["direction_id"]).astype(
        "int64"
    )

    # store start_time as seconds from start of day int64
    events["start_time"] = (
        events["start_time"].apply(start_time_to_seconds).astype("int64")
    )

    # expand and filter stop_time_update column using numpy vectorize
    # numpy vectorize offers significantly better performance over pandas apply
    # this will return a ndarray with values being list of dicts
    vector_explode = numpy.vectorize(explode_stop_time_update)
    events = pandas.Series(
        vector_explode(
            events.stop_time_update,
            events.timestamp,
            events.direction_id,
            events.route_id,
            events.start_date,
            events.start_time,
            events.vehicle_id,
        )
    ).dropna()

    # transform Series of list of dicts into dataframe
    events = pandas.json_normalize(events.explode())

    # is_moving column to indicate all stop events
    # needed for later join with vehicle_position_event by hash
    events["is_moving"] = False
    events["pk_id"] = None

    # add hash column, hash should be consistent across trip_update and
    # vehicle_position events
    events = add_event_hash_column(events).sort_values(by=["hash", "timestamp"])

    # after sort, drop all duplicates by hash, keep last record
    # last record will be most recent arrival time prediction for event
    events = events.drop_duplicates(subset=["hash"], keep="last")

    # after sort and drop timestamp column no longer needed
    events = events.drop(columns=["timestamp"])

    return events


def merge_trip_update_events(  # pylint: disable=too-many-locals
    new_events: pandas.DataFrame, session: sa.orm.session.sessionmaker
) -> None:
    """
    merge new trip update evetns with existing events found in database
    merge performed on hash of records
    """
    process_logger = ProcessLogger("tu_merge_events")
    process_logger.log_start()

    hash_list = new_events["hash"].tolist()
    get_db_events = sa.select(
        (
            TripUpdateEvents.pk_id,
            TripUpdateEvents.hash,
            TripUpdateEvents.timestamp_start,
        )
    ).where(TripUpdateEvents.hash.in_(hash_list))

    with session.begin() as curosr:
        merge_events = pandas.concat(
            [
                pandas.DataFrame(
                    [r._asdict() for r in curosr.execute(get_db_events)]
                ),
                new_events,
            ]
        ).sort_values(by=["hash", "timestamp_start"])

    # pylint: disable=duplicate-code
    # TODO(zap): the following code is duplicated in rt_vehicle_positions.py

    # Identify records that are continuing from existing db
    # If such records are found, update timestamp_end with latest value
    first_of_consecutive_events = merge_events["hash"] == merge_events[
        "hash"
    ].shift(-1)
    last_of_consecutive_events = merge_events["hash"] == merge_events[
        "hash"
    ].shift(1)

    merge_events["timestamp_start"] = numpy.where(
        first_of_consecutive_events,
        merge_events["timestamp_start"].shift(-1),
        merge_events["timestamp_start"],
    )

    existing_was_updated_mask = (
        ~(merge_events["pk_id"].isna()) & first_of_consecutive_events
    )

    existing_to_del_mask = (
        ~(merge_events["pk_id"].isna()) & last_of_consecutive_events
    )

    # new events that will be inserted into db table
    new_to_insert_mask = (
        merge_events["pk_id"].isna()
    ) & ~last_of_consecutive_events

    # add counts to process logger metadata
    process_logger.add_metadata(
        updated_count=existing_was_updated_mask.sum(),
        deleted_count=existing_to_del_mask.sum(),
        inserted_count=new_to_insert_mask.sum(),
    )
    # pylint: enable=duplicate-code

    # DB UPDATE operation
    if existing_was_updated_mask.sum() > 0:
        update_db_events = sa.update(TripUpdateEvents.__table__).where(
            TripUpdateEvents.pk_id == sa.bindparam("b_pk_id")
        )
        with session.begin() as cursor:
            cursor.execute(
                update_db_events,
                merge_events.rename(columns={"pk_id": "b_pk_id"})
                .loc[existing_was_updated_mask, ["b_pk_id", "timestamp_start"]]
                .to_dict(orient="records"),
            )

    # DB DELETE operation
    if existing_to_del_mask.sum() > 0:
        delete_db_events = sa.delete(TripUpdateEvents.__table__).where(
            TripUpdateEvents.pk_id.in_(
                merge_events.loc[existing_to_del_mask, "pk_id"]
            )
        )
        with session.begin() as cursor:
            cursor.execute(delete_db_events)

    # DB INSERT operation
    if new_to_insert_mask.sum() > 0:
        insert_cols = list(set(merge_events.columns) - {"pk_id"})
        with session.begin() as cursor:
            cursor.execute(
                sa.insert(TripUpdateEvents.__table__),
                merge_events.loc[new_to_insert_mask, insert_cols].to_dict(
                    orient="records"
                ),
            )

    process_logger.log_complete()


def process_trip_updates(db_manager: DatabaseManager) -> None:
    """
    process trip updates parquet files from metadataLog table
    """
    process_logger = ProcessLogger("process_tu")
    process_logger.log_start()

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files(
        "RT_TRIP_UPDATES", db_manager.get_session()
    )

    process_logger.add_metadata(paths_to_load=len(paths_to_load))

    for folder_data in paths_to_load.values():
        ids = folder_data["ids"]
        paths = folder_data["paths"]

        subprocess_logger = ProcessLogger(
            "process_tu_dir", file_count=len(paths)
        )
        subprocess_logger.log_start()

        try:
            sizes = {}
            new_events = get_tu_dataframe(paths)
            sizes["new_events_size"] = new_events.shape[0]

            new_events = unwrap_tu_dataframe(new_events)
            sizes["new_events_unwrapped_size"] = new_events.shape[0]

            subprocess_logger.add_metadata(**sizes)

            merge_trip_update_events(
                new_events=new_events,
                session=db_manager.get_session(),
            )
        except Exception as exception:
            subprocess_logger.log_failure(exception)
        else:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            db_manager.execute(update_md_log)
            subprocess_logger.log_complete()

    process_logger.log_complete()
