from typing import (
    Optional, 
    Union, 
    List, 
    Dict, 
    Any, 
    Iterator,
)

import pathlib
import pandas
import numpy

import sqlalchemy as sa

from .gtfs_utils import start_time_to_seconds, add_event_hash_column
from .logging_utils import ProcessLogger
from .postgres_schema import (
    TripUpdateEvents,
    MetadataLog,
    StaticFeedInfo,
    StaticStops,
)
from .postgres_utils import get_unprocessed_files, DatabaseManager
from .s3_utils import read_parquet_chunks


def get_tu_dataframe_chunks(
    to_load: Union[str, List[str]]
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
    ]
    trip_update_filters = [
        ("direction_id", "in", (0, 1)),
        ("timestamp", ">", 0),
        ("route_id", "!=", "None"),
        ("start_date", "!=", "None"),
        ("start_time", "!=", "None"),
        ("vehicle_id", "!=", "None"),
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


def get_and_unwrap_tu_dataframe(
    paths: Union[str, List[str]]
) -> pandas.DataFrame:
    """
    unwrap and explode trip updates records from parquet files
    parquet files contain stop_time_update field that is saved as list of dicts
    stop_time_update must have fields extracted and flattened to create
    predicted trip update stop events
    """
    events = pandas.Series(dtype="object")
    # get_tu_dataframe_chunks set to pull ~100_000 trip update records
    # per batch, this should result in ~5-6 GB of memory use per batch
    # after batch goes through explod_stop_time_update vectorize operation,
    # resulting Series has negligible memory use
    for batch_events in get_tu_dataframe_chunks(paths):
        # store start_date as int64
        batch_events["start_date"] = pandas.to_numeric(
            batch_events["start_date"]
        ).astype("int64")

        # store direction_id as int64
        batch_events["direction_id"] = pandas.to_numeric(
            batch_events["direction_id"]
        ).astype("int8")

        # store start_time as seconds from start of day int64
        batch_events["start_time"] = (
            batch_events["start_time"]
            .apply(start_time_to_seconds)
            .astype("int64")
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
                batch_events.start_date,
                batch_events.start_time,
                batch_events.vehicle_id,
            )
        ).dropna()
        events = pandas.concat([events, batch_events])

    # transform Series of list of dicts into dataframe
    events = pandas.json_normalize(events.explode())

    # is_moving column to indicate all stop events
    # needed for later join with vehicle_position_event by hash
    events["is_moving"] = False
    events["pk_id"] = None

    return events


def join_gtfs_static(
    trip_updates: pandas.DataFrame, db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    join trip update dataframe to gtfs static records

    adds "fk_static_timestamp" and "parent_station" columns to dataframe
    """
    # get unique "start_date" values from trip update dataframe
    # with associated minimum "timestamp_start"
    date_groups = trip_updates.groupby(by="start_date")["timestamp_start"].min()

    # pylint: disable=duplicate-code
    # TODO(ryan): the following code is duplicated in rt_vehicle_positions.py

    # dictionary used to match minimum "timestamp_start" values to
    # "timestamp" from StaticFeedInfo table, to be used as foreign key
    timestamp_lookup = {}
    for (date, min_timestamp) in date_groups.iteritems():
        date = int(date)
        min_timestamp = int(min_timestamp)
        # "start_date" from trip updates must be between "feed_start_date"
        # and "feed_end_date" in StaticFeedInfo
        # minimum "timestamp_start" must also be less than "timestamp" from
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
        # exists for this trip update data, so the tri update data
        # should not be processed until valid static schedule data exists
        if len(result) == 0:
            raise IndexError(
                f"StaticFeedInfo table has no matching schedule for start_date={date}, timestamp={min_timestamp}"
            )
        timestamp_lookup[min_timestamp] = int(result[0]["timestamp"])

    # pylint: enable=duplicate-code

    trip_updates["fk_static_timestamp"] = timestamp_lookup[
        min(timestamp_lookup.keys())
    ]
    # add "fk_static_timestamp" column to trip update dataframe
    # loop is to handle batches vehicle position batches that are applicable to
    # overlapping static gtfs data
    for min_timestamp in sorted(timestamp_lookup.keys()):
        timestamp_mask = trip_updates["timestamp_start"] >= min_timestamp
        trip_updates.loc[
            timestamp_mask, "fk_static_timestamp"
        ] = timestamp_lookup[min_timestamp]

    # unique list of "fk_static_timestamp" values for pulling parent stations
    static_timestamps = list(set(timestamp_lookup.values()))

    # pull parent station data for joining to trip update events
    parent_station_query = sa.select(
        StaticStops.timestamp.label("fk_static_timestamp"),
        StaticStops.stop_id,
        StaticStops.parent_station,
    ).where(StaticStops.timestamp.in_(static_timestamps))
    parent_stations = db_manager.select_as_dataframe(parent_station_query)

    # join parent stations to trip updates on "stop_id" and gtfs static
    # timestamp foreign key
    trip_updates = trip_updates.merge(
        parent_stations, how="left", on=["fk_static_timestamp", "stop_id"]
    )
    # is parent station is not provided, transfer "stop_id" value to
    # "parent_station" column
    trip_updates["parent_station"] = numpy.where(
        trip_updates["parent_station"].isna(),
        trip_updates["stop_id"],
        trip_updates["parent_station"],
    )

    return trip_updates


def hash_events(trip_updates: pandas.DataFrame) -> pandas.DataFrame:
    """
    hash category columns of rows and return data frame with "hash" column
    """
    # add hash column, hash should be consistent across trip_update and
    # vehicle_position events
    trip_updates = add_event_hash_column(trip_updates).sort_values(
        by=["hash", "timestamp"]
    )

    # after sort, drop all duplicates by hash, keep last record
    # last record will be most recent arrival time prediction for event
    trip_updates = trip_updates.drop_duplicates(subset=["hash"], keep="last")

    # after hash and sort, "timestamp" and "parent_station" no longer needed
    trip_updates = trip_updates.drop(columns=["timestamp", "parent_station"])

    return trip_updates


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
    process_logger = ProcessLogger(
        "l0_tables_loader", table_type="rt_trip_update"
    )
    process_logger.log_start()

    # pull list of objects that need processing from metadata table
    paths_to_load = get_unprocessed_files(
        "RT_TRIP_UPDATES", db_manager, file_limit=6
    )

    process_logger.add_metadata(count_of_paths=len(paths_to_load))

    for folder_data in paths_to_load:
        folder = str(pathlib.Path(folder_data["paths"][0]).parent)
        ids = folder_data["ids"]
        paths = folder_data["paths"]

        subprocess_logger = ProcessLogger(
            "l0_load_table",
            table_type="rt_trip_update",
            file_count=len(paths),
            s3_path=folder,
        )
        subprocess_logger.log_start()

        try:
            sizes = {}

            new_events = get_and_unwrap_tu_dataframe(paths)
            new_events_records = new_events.shape[0]
            sizes["merge_events_count"] = new_events_records

            # skip processing if no new records in file
            if new_events_records > 0:
                new_events = join_gtfs_static(new_events, db_manager)

                new_events = hash_events(new_events)

                merge_trip_update_events(
                    new_events=new_events,
                    session=db_manager.get_session(),
                )

            subprocess_logger.add_metadata(**sizes)

            # same found in l0_rt_vehicle_positions.py
            # pylint: disable=duplicate-code
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            db_manager.execute(update_md_log)
            subprocess_logger.log_complete()
        except Exception as exception:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1, process_fail=1)
            )
            db_manager.execute(update_md_log)
            subprocess_logger.log_failure(exception)
        # pylint: enable=duplicate-code

    process_logger.log_complete()
