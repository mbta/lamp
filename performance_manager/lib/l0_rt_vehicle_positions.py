from typing import List, Union

import pathlib
import numpy
import pandas
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from .gtfs_utils import start_time_to_seconds, add_event_hash_column
from .logging_utils import ProcessLogger
from .postgres_schema import (
    StaticStops,
    VehiclePositionEvents,
    MetadataLog,
    StaticFeedInfo,
)
from .postgres_utils import get_unprocessed_files, DatabaseManager
from .s3_utils import read_parquet, get_utc_from_partition_path


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

    return read_parquet(to_load, columns=vehicle_position_cols)


def transform_vp_dtyes(vehicle_positions: pandas.DataFrame) -> pandas.DataFrame:
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


def join_gtfs_static(
    vehicle_positions: pandas.DataFrame, db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    join vehicle position dataframe to gtfs static records

    adds "fk_static_timestamp" and "parent_station" columns to dataframe
    """
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

    return vehicle_positions


def hash_events(vehicle_positions: pandas.DataFrame) -> pandas.DataFrame:
    """
    hash category columns of rows and return data frame with "hash" column
    """
    # hash row data, exluding vehicle_timestamp column
    # This hash is used to identify continuous series of records with the same
    # categorical data
    vehicle_positions = add_event_hash_column(vehicle_positions)

    # drop duplicates of hash and vehicle_timestamp columns (extraneous data)
    vehicle_positions = vehicle_positions.drop_duplicates(
        subset=["vehicle_timestamp", "hash"]
    )

    # "parent_station" column no longer needed, can be dropped
    vehicle_positions = vehicle_positions.drop(columns=["parent_station"])

    return vehicle_positions


def transform_vp_timestamps(
    vehicle_positions: pandas.DataFrame,
) -> pandas.DataFrame:
    """
    convert raw vehicle position data with one timestamp field into
    vehicle position events with timestamp_start and timestamp_end
    """
    vehicle_positions = vehicle_positions.sort_values(
        by=["vehicle_id", "vehicle_timestamp"]
    )

    # calculate mask to identify time series hash changes
    # this keeps first and last event for matching consecutive hash events
    # result is a 'start' event and 'end' event in consecutive rows for
    # matching hash events
    first_last_mask = (
        vehicle_positions["hash"] != vehicle_positions["hash"].shift(1)
    ) | (vehicle_positions["hash"] != vehicle_positions["hash"].shift(-1))
    vehicle_positions = vehicle_positions.loc[first_last_mask, :]

    # transform vehicle_timestamp with matching consectuive hash events into
    # one record with timestamp_start and timestamp_end columns if event is not
    # part of consectuive matching events, then timestamp_start and
    # timestamp_end will be same value
    vehicle_positions.rename(
        columns={"vehicle_timestamp": "timestamp_start"}, inplace=True
    )
    vehicle_positions["timestamp_end"] = numpy.where(
        (vehicle_positions["hash"] == vehicle_positions["hash"].shift(-1)),
        vehicle_positions["timestamp_start"].shift(-1),
        vehicle_positions["timestamp_start"],
    ).astype("int64")

    # for matching consectuive hash events, drop 2nd event because timestamp has
    # been captured in timestamp_end value of previous event
    drop_last_mask = vehicle_positions["hash"] != vehicle_positions[
        "hash"
    ].shift(1)
    vehicle_positions = vehicle_positions.loc[drop_last_mask, :].reset_index(
        drop=True
    )

    vehicle_positions.insert(0, "pk_id", numpy.nan)

    # fill all dataframe na values with Python None (for DB writing)
    vehicle_positions = vehicle_positions.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    return vehicle_positions


def get_event_overlap(
    folder: str,
    new_events: pandas.DataFrame,
    sql_session: sessionmaker,
) -> pandas.DataFrame:
    """
    get a dataframe consisting of the new events and any overlapping events from
    the vehicle positions events table, sorted by vehicle id and then timestamp
    """
    start_window_ts = get_utc_from_partition_path(folder)
    timestamp_to_pull_min = start_window_ts - 300
    timestamp_to_pull_max = start_window_ts + 300 + 3600
    get_db_events = sa.select(
        (
            VehiclePositionEvents.pk_id,
            VehiclePositionEvents.hash,
            VehiclePositionEvents.timestamp_start,
            VehiclePositionEvents.timestamp_end,
            VehiclePositionEvents.vehicle_id,
        )
    ).where(
        (VehiclePositionEvents.timestamp_end > timestamp_to_pull_min)
        & (VehiclePositionEvents.timestamp_start < timestamp_to_pull_max)
    )

    with sql_session.begin() as session:
        # Join records from db with records from parquet db records should
        # contain 'index' column that does not exist in parquet dataset
        return pandas.concat(
            [
                pandas.DataFrame(
                    [row._asdict() for row in session.execute(get_db_events)]
                ),
                new_events,
            ]
        ).sort_values(by=["vehicle_id", "timestamp_start"])


def merge_vehicle_position_events(
    folder: str,
    new_events: pandas.DataFrame,
    session: sessionmaker,
) -> None:
    """
    merge new events from parquet file with exiting events found in database
    new events will be merged with existing events in a 5 minutes window
    surrounding the year/month/day/hour value found in path of parquet files
    """
    process_logger = ProcessLogger("vp_merge_events")
    process_logger.log_start()

    merge_events = get_event_overlap(folder, new_events, session)

    # pylint: disable=duplicate-code

    # Identify records that are continuing from existing db
    # If such records are found, update timestamp_end with latest value
    first_of_consecutive_events = merge_events["hash"] == merge_events[
        "hash"
    ].shift(-1)
    last_of_consecutive_events = merge_events["hash"] == merge_events[
        "hash"
    ].shift(1)

    merge_events["timestamp_end"] = numpy.where(
        first_of_consecutive_events,
        merge_events["timestamp_end"].shift(-1),
        merge_events["timestamp_end"],
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
        update_db_events = sa.update(VehiclePositionEvents.__table__).where(
            VehiclePositionEvents.pk_id == sa.bindparam("b_pk_id")
        )
        with session.begin() as cursor:
            cursor.execute(
                update_db_events,
                merge_events.rename(columns={"pk_id": "b_pk_id"})
                .loc[existing_was_updated_mask, ["b_pk_id", "timestamp_end"]]
                .to_dict(orient="records"),
            )

    # DB DELETE operation
    if existing_to_del_mask.sum() > 0:
        delete_db_events = sa.delete(VehiclePositionEvents.__table__).where(
            VehiclePositionEvents.pk_id.in_(
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
                sa.insert(VehiclePositionEvents.__table__),
                merge_events.loc[new_to_insert_mask, insert_cols].to_dict(
                    orient="records"
                ),
            )

    process_logger.log_complete()


def process_vehicle_positions(db_manager: DatabaseManager) -> None:
    """
    process a bunch of vehicle position files
    create events for them
    merge those events with existing events
    """
    process_logger = ProcessLogger(
        "l0_tables_loader", table_type="rt_vehicle_position"
    )
    process_logger.log_start()

    # check metadata table for unprocessed parquet files
    paths_to_load = get_unprocessed_files(
        "RT_VEHICLE_POSITIONS", db_manager, file_limit=6
    )
    process_logger.add_metadata(count_of_paths=len(paths_to_load))

    for folder_data in paths_to_load:
        folder = str(pathlib.Path(folder_data["paths"][0]).parent)
        ids = folder_data["ids"]
        paths = folder_data["paths"]

        subprocess_logger = ProcessLogger(
            "l0_load_table",
            table_type="rt_vehicle_position",
            s3_path=folder,
            file_count=len(paths),
        )
        subprocess_logger.log_start()

        try:
            sizes = {}
            new_events = get_vp_dataframe(paths)
            sizes["parquet_events_count"] = new_events.shape[0]

            new_events = transform_vp_dtyes(new_events)

            new_events = join_gtfs_static(new_events, db_manager)

            new_events = hash_events(new_events)

            new_events = transform_vp_timestamps(new_events)
            sizes["merge_events_count"] = new_events.shape[0]

            subprocess_logger.add_metadata(**sizes)

            merge_vehicle_position_events(
                str(folder), new_events, db_manager.get_session()
            )

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

    process_logger.log_complete()
