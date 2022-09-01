import logging
import datetime
import re

from typing import List, Union

import numpy
import pandas
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from .s3_utils import read_parquet
from .postgres_utils import (
    get_unprocessed_files,
    DatabaseManager,
)
from .postgres_schema import VehiclePositionEvents, MetadataLog
from .gtfs_utils import start_time_to_seconds, add_event_hash_column


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

    # hash row data, exluding vehicle_timestamp column
    # This hash is used to identify continuous series of records with the same
    # categorical data
    vehicle_positions = add_event_hash_column(vehicle_positions)

    # drop duplicates of hash and vehicle_timestamp columns (extraneous data)
    vehicle_positions = vehicle_positions.drop_duplicates(
        subset=["vehicle_timestamp", "hash"]
    )

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
        vehicle_positions["hash"] - vehicle_positions["hash"].shift(1) != 0
    ) | (vehicle_positions["hash"] - vehicle_positions["hash"].shift(-1) != 0)
    vehicle_positions = vehicle_positions.loc[first_last_mask, :]

    # transform vehicle_timestamp with matching consectuive hash events into
    # one record with timestamp_start and timestamp_end columns if event is not
    # part of consectuive matching events, then timestamp_start and
    # timestamp_end will be same value
    vehicle_positions.rename(
        columns={"vehicle_timestamp": "timestamp_start"}, inplace=True
    )
    vehicle_positions["timestamp_end"] = numpy.where(
        (vehicle_positions["hash"] - vehicle_positions["hash"].shift(-1) == 0),
        vehicle_positions["timestamp_start"].shift(-1),
        vehicle_positions["timestamp_start"],
    ).astype("int64")

    # for matching consectuive hash events, drop 2nd event because timestamp has
    # been captured in timestamp_end value of previous event
    drop_last_mask = (
        vehicle_positions["hash"] - vehicle_positions["hash"].shift(1) != 0
    )
    vehicle_positions = vehicle_positions.loc[drop_last_mask, :].reset_index(
        drop=True
    )

    vehicle_positions.insert(0, "pk_id", numpy.nan)

    # fill all dataframe na values with Python None (for DB writing)
    vehicle_positions = vehicle_positions.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    return vehicle_positions


def get_utc_timestamp(path: str) -> float:
    """
    process datetime from s3 path return UTC timestamp
    """
    year = int(re.findall(r"year=(\d{4})", path)[0])
    month = int(re.findall(r"month=(\d{1,2})", path)[0])
    day = int(re.findall(r"day=(\d{1,2})", path)[0])
    hour = int(re.findall(r"hour=(\d{1,2})", path)[0])
    date = datetime.datetime(
        year=year, month=month, day=day, hour=hour, tzinfo=datetime.timezone.utc
    )
    return datetime.datetime.timestamp(date)


def get_event_overlap(
    folder: str,
    new_events: pandas.DataFrame,
    sql_session: sessionmaker,
) -> pandas.DataFrame:
    """
    get a dataframe consisting of the new events and any overlapping events from
    the vehicle positions events table, sorted by vehicle id and then timestamp
    """
    start_window_ts = get_utc_timestamp(folder)
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
    merge_events = get_event_overlap(folder, new_events, session)

    # Identify records that are continuing from existing db
    # If such records are found, update timestamp_end with latest value
    first_of_consecutive_events = (
        merge_events["hash"] - merge_events["hash"].shift(-1) == 0
    )
    last_of_consecutive_events = (
        merge_events["hash"] - merge_events["hash"].shift(1) == 0
    )
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

    logging.info(
        "Size of existing update vehicle_positions: %d",
        existing_was_updated_mask.sum(),
    )
    logging.info(
        "Size of existing delete vehicle_positions: %d",
        existing_to_del_mask.sum(),
    )

    # new events that will be inserted into db table
    new_to_insert_mask = (
        merge_events["pk_id"].isna()
    ) & ~last_of_consecutive_events
    logging.info(
        "Size of new insert vehicle_positions: %d", new_to_insert_mask.sum()
    )

    # new events that were used to update existing records, will not be used
    new_to_drop_mask = (
        merge_events["pk_id"].isna()
    ) & last_of_consecutive_events
    logging.info(
        "Size of new being dropped vehicle_positions: %d",
        new_to_drop_mask.sum(),
    )

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


def process_vehicle_positions(db_manager: DatabaseManager) -> None:
    """
    process a bunch of vehicle position files
    create events for them
    merge those events with existing events
    """

    # check metadata table for unprocessed parquet files
    paths_to_load = get_unprocessed_files(
        "RT_VEHICLE_POSITIONS", db_manager.get_session()
    )

    for folder, folder_data in paths_to_load.items():
        ids = folder_data["ids"]
        paths = folder_data["paths"]

        try:
            new_events = get_vp_dataframe(paths)
            logging.info(
                "Size of dataframe from %s is %d", folder, new_events.shape[0]
            )
            new_events = transform_vp_dtyes(new_events)
            logging.info(
                "Size of dataframe with updated dtypes is %d",
                new_events.shape[0],
            )
            new_events = transform_vp_timestamps(new_events)
            logging.info(
                "Size of dataframe with transformed timestamps is %d",
                new_events.shape[0],
            )

            merge_vehicle_position_events(
                str(folder), new_events, db_manager.get_session()
            )
        except Exception as e:
            logging.info("Error Processing Vehicle Positions")
            logging.exception(e)
        else:
            update_md_log = (
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(ids))
                .values(processed=1)
            )
            db_manager.execute(update_md_log)
