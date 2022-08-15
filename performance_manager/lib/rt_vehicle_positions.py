import logging

import pandas
import numpy
import sqlalchemy
import re
import datetime

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from typing import Optional
from typing import Union
from typing import List

from .s3_utils import read_parquet
from .postgres_utils import VehiclePositionEvents


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


def start_time_to_seconds(
    time: Optional[str],
) -> Optional[float]:
    """
    transform time string in HH:MM:SS format to seconds integer
    """
    if time is None:
        return time
    (hour, min, sec) = time.split(":")
    return int(hour) * 3600 + int(min) * 60 + int(sec)


def transform_vp_dtyes(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    ingest dataframe of vehicle position data from parquet file and transform
    column datatypes
    """
    # current_staus: 1 = MOVING, 0 = STOPPED_AT
    df["is_moving"] = numpy.where(
        df["current_status"] != "STOPPED_AT", True, False
    ).astype(numpy.bool8)
    df = df.drop(columns=["current_status"])

    # store start_date as Int64 [nullable] instead of string
    df["start_date"] = pandas.to_numeric(df["start_date"]).astype("Int64")

    # store current_stop_sequence as Int64 [nullable]
    df["current_stop_sequence"] = pandas.to_numeric(
        df["current_stop_sequence"]
    ).astype("Int64")

    # store direction_id as Int64 [nullable]
    df["direction_id"] = pandas.to_numeric(df["direction_id"]).astype("Int64")

    # store start_time as seconds from start of day (Int64 [nullable])
    df["start_time"] = (
        df["start_time"].apply(start_time_to_seconds).astype("Int64")
    )

    # hash row data, exluding vehicle_timestamp column
    # This hash is used to identify continuous series of records with the same
    # categorical data
    df["hash"] = df[list(set(df.columns) - {"vehicle_timestamp"})].apply(
        lambda row: hash(tuple(row)), axis=1
    )

    # drop duplicates of hash and vehicle_timestamp columns (extraneous data)
    df = df.drop_duplicates(subset=["vehicle_timestamp", "hash"])

    return df


def transform_vp_timestamps(df: pandas.DataFrame) -> pandas.DataFrame:
    """
    convert raw vehicle position data with one timestamp field into
    vehicle position events with timestamp_start and timestamp_end
    """
    df = df.sort_values(by=["vehicle_id", "vehicle_timestamp"])

    """
    calculate mask to identify time series hash changes
    this keeps first and last event for matching consecutive hash events
    result is a 'start' event and 'end' event in consecutive rows for 
    matching hash events
    """
    first_last_mask = (df["hash"] - df["hash"].shift(1) != 0) | (
        df["hash"] - df["hash"].shift(-1) != 0
    )
    df = df.loc[first_last_mask, :]

    """
    transform vehicle_timestamp with matching consectuive hash events
    into one record with timestamp_start and timestamp_end columns
    if event is not part of consectuive matching events, then 
    timestamp_start and timestamp_end will be same value
    """
    df.rename(columns={"vehicle_timestamp": "timestamp_start"}, inplace=True)
    df["timestamp_end"] = numpy.where(
        (df["hash"] - df["hash"].shift(-1) == 0),
        df["timestamp_start"].shift(-1),
        df["timestamp_start"],
    ).astype("int64")

    """
    for matching consectuive hash events, drop 2nd event because timestamp
    has been captured in timestamp_end value of previous event
    """
    drop_last_mask = df["hash"] - df["hash"].shift(1) != 0
    df = df.loc[drop_last_mask, :].reset_index(drop=True)

    df.insert(0, "pk_id", numpy.nan)

    # fill all dataframe na values with Python None (for DB writing)
    df = df.fillna(numpy.nan).replace([numpy.nan], [None])

    return df


def get_utc_timestamp(path: str) -> float:
    """
    process datetime from s3 path return UTC timestamp
    """
    year = int(re.findall(r"year=(\d{4})", path)[0])
    month = int(re.findall(r"month=(\d{1,2})", path)[0])
    day = int(re.findall(r"day=(\d{1,2})", path)[0])
    hour = int(re.findall(r"hour=(\d{1,2})", path)[0])
    dt = datetime.datetime(
        year=year, month=month, day=day, hour=hour, tzinfo=datetime.timezone.utc
    )
    return datetime.datetime.timestamp(dt)


def merge_vehicle_position_events(
    folder: str,
    new_events: pandas.DataFrame,
    session: sqlalchemy.orm.session.sessionmaker,
) -> None:
    """
    merge new events from parquet file with exiting events found in database
    new events will be merged with existing events in a 5 minutes window
    surrounding the year/month/day/hour value found in path of parquet files
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
        (VehiclePositionEvents.timestamp_start > timestamp_to_pull_min)
        & (VehiclePositionEvents.timestamp_end < timestamp_to_pull_max)
    )
    with session.begin() as s:  # type: ignore
        if s.execute(get_db_events).first() is None:
            logging.info(f"No merge events found in database.")
            merge_events = new_events.sort_values(
                by=["vehicle_id", "timestamp_start"]
            )
        else:
            # Join records from db with records from parquet
            # db records should contain 'index' column that does not exist in parquet dataset
            merge_events = pandas.concat(
                [
                    pandas.DataFrame(
                        [r._asdict() for r in s.execute(get_db_events)]
                    ),
                    new_events,
                ]
            ).sort_values(by=["vehicle_id", "timestamp_start"])

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
        f"Size of existing update df: {existing_was_updated_mask.sum():,}"
    )
    logging.info(f"Size of existing delete df: {existing_to_del_mask.sum():,}")

    """
    new events that will be inserted into db table
    """
    new_to_insert_mask = (
        merge_events["pk_id"].isna()
    ) & ~last_of_consecutive_events
    logging.info(f"Size of new insert df: {new_to_insert_mask.sum():,}")

    """
    new events that were used to update existing records, will not be used
    """
    new_to_drop_mask = (
        merge_events["pk_id"].isna()
    ) & last_of_consecutive_events
    logging.info(f"Size of new being dropped df: {new_to_drop_mask.sum():,}")

    """
    DB UPDATE operation
    """
    if existing_was_updated_mask.sum() > 0:
        update_db_events = sa.update(VehiclePositionEvents.__table__).where(
            VehiclePositionEvents.pk_id == sqlalchemy.bindparam("b_pk_id")
        )
        with session.begin() as s:  # type: ignore
            s.execute(
                update_db_events,
                merge_events.rename(columns={"pk_id": "b_pk_id"})
                .loc[existing_was_updated_mask, ["b_pk_id", "timestamp_end"]]
                .to_dict(orient="records"),
            )

    """
    DB DELETE operation
    """
    if existing_to_del_mask.sum() > 0:
        delete_db_events = sa.delete(VehiclePositionEvents.__table__).where(
            VehiclePositionEvents.pk_id.in_(
                merge_events.loc[existing_to_del_mask, "pk_id"]
            )
        )
        with session.begin() as s:  # type: ignore
            s.execute(delete_db_events)

    """
    DB INSERT operation
    """
    if new_to_insert_mask.sum() > 0:
        insert_cols = list(set(merge_events.columns) - {"pk_id"})
        with session.begin() as s:  # type: ignore
            s.execute(
                sa.insert(VehiclePositionEvents.__table__),
                merge_events.loc[new_to_insert_mask, insert_cols].to_dict(
                    orient="records"
                ),
            )
