import binascii
from typing import Dict, List

import numpy
import pandas
import sqlalchemy as sa

from lamp_py.aws.ecs import check_for_sigterm
from lamp_py.aws.s3 import get_utc_from_partition_path
from lamp_py.postgres.postgres_schema import (
    MetadataLog,
    TempHashCompare,
    VehicleEvents,
)
from lamp_py.postgres.postgres_utils import (
    DatabaseManager,
    get_unprocessed_files,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import add_event_hash_column
from .l0_rt_trip_updates import process_tu_files
from .l0_rt_vehicle_positions import process_vp_files


def get_gtfs_rt_paths(db_manager: DatabaseManager) -> List[Dict[str, List]]:
    """
    get all of the gtfs_rt files and group them by the hour they are from
    """
    process_logger = ProcessLogger("gtfs_rt.get_files")
    process_logger.log_start()

    grouped_files = {}

    vp_files = get_unprocessed_files("RT_VEHICLE_POSITIONS", db_manager)
    for record in vp_files:
        timestamp = get_utc_from_partition_path(record["paths"][0])
        grouped_files[timestamp] = {
            "ids": record["ids"],
            "vp_paths": record["paths"],
            "tu_paths": [],
        }

    tu_files = get_unprocessed_files("RT_TRIP_UPDATES", db_manager)
    for record in tu_files:
        timestamp = get_utc_from_partition_path(record["paths"][0])
        if timestamp in grouped_files:
            grouped_files[timestamp]["ids"] += record["ids"]
            grouped_files[timestamp]["tu_paths"] += record["paths"]
        else:
            grouped_files[timestamp] = {
                "ids": record["ids"],
                "tu_paths": record["paths"],
                "vp_paths": [],
            }

    process_logger.add_metadata(hours_found=len(grouped_files))
    process_logger.log_complete()

    return [
        grouped_files[timestamp] for timestamp in sorted(grouped_files.keys())
    ]


def combine_events(
    vp_events: pandas.DataFrame, tu_events: pandas.DataFrame
) -> pandas.DataFrame:
    """
    collapse the vp events and tu events into a single vehicle events df

    the vehicle events will have one or both of vp_move_time and
    vp_stop_time entries. the trip updates events will have a
    tu_stop_time entry. many of these events should have overlap in their
    trip_stop_hash entries, implying they should be associated together.

    join the dataframes and collapse rows representing the same trip and
    stop pairs.
    """
    process_logger = ProcessLogger(
        "gtfs_rt.combine_events",
        vp_event_count=vp_events.shape[0],
        tu_event_count=tu_events.shape[0],
    )
    process_logger.log_start()

    # merge together the trip stop hashes and the timestamps
    events = pandas.merge(
        vp_events[["trip_stop_hash", "vp_stop_timestamp", "vp_move_timestamp"]],
        tu_events[["trip_stop_hash", "tu_stop_timestamp"]],
        how="outer",
        on="trip_stop_hash",
        validate="one_to_one",
    )

    # concat all of the details that went into each trip stop hash, dropping
    # duplicates. this is now a dataframe mapping hashes back to the things
    # that went into them.
    details_columns = [
        "direction_id",
        "fk_static_timestamp",
        "parent_station",
        "route_id",
        "start_date",
        "start_time",
        "stop_id",
        "stop_sequence",
        "trip_stop_hash",
        "vehicle_id",
    ]

    event_details = pandas.concat(
        [vp_events[details_columns], tu_events[details_columns]]
    ).drop_duplicates(subset="trip_stop_hash")

    # pull the details and add them to the events table
    events = events.merge(
        event_details, how="left", on="trip_stop_hash", validate="one_to_one"
    )

    process_logger.add_metadata(total_event_count=events.shape[0])
    process_logger.log_complete()

    return events


def compute_metrics(events: pandas.DataFrame) -> pandas.DataFrame:
    """
    generate dwell times, stop times, and headways metrics for events
    """
    # TODO(zap / ryan) - figure out how 2 compute these. the update
    # logic will also need to be updated.
    return events


def upload_to_database(
    events: pandas.DataFrame, db_manager: DatabaseManager
) -> None:
    """
    add vehicle event data to the database

    pull existing events from the database that overlap with the proccessed
    events. split the processed events into those whose trip_stop_hash is
    already in the database and those that are brand new. update preexisting
    events where appropriate and insert the new ones.
    """
    # handle empty events dataframe
    if events.shape[0] == 0:
        return

    # add in column for trip hash that will be unique to the trip. this will
    # be useful for metrics and querries users want to run.
    events = add_event_hash_column(
        events,
        hash_column_name="trip_hash",
        expected_hash_columns=[
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
        ],
    )
    events["trip_hash"] = events["trip_hash"].str.decode("hex")

    process_logger = ProcessLogger(
        "gtfs_rt.pull_overlapping_events",
        event_count=events.shape[0],
    )
    process_logger.log_start()

    # remove everything from the temp hash table and insert the trip stop hashs
    # from the new events. then pull events from the VehicleEvents table by
    # matching those hashes, which will be the events that will potentially be
    # updated. re-label vp timestamp columns to compare them after a left merge.
    hash_bytes = events["trip_stop_hash"].str.decode("hex")
    db_manager.truncate_table(TempHashCompare)
    db_manager.execute_with_data(
        sa.insert(TempHashCompare.__table__),
        hash_bytes.to_frame("trip_stop_hash"),
    )

    database_events = db_manager.select_as_dataframe(
        sa.select(
            VehicleEvents.pk_id,
            VehicleEvents.trip_stop_hash,
            VehicleEvents.vp_move_timestamp.label("vp_move_db"),
            VehicleEvents.vp_stop_timestamp.label("vp_stop_db"),
        ).join(
            TempHashCompare,
            TempHashCompare.trip_stop_hash == VehicleEvents.trip_stop_hash,
        )
    )

    if database_events.shape[0] == 0:
        database_events = pandas.DataFrame(
            columns=["pk_id", "trip_stop_hash", "vp_move_db", "vp_stop_db"]
        )
    else:
        # convert the trip stop hash bytes to hex
        database_events["trip_stop_hash"] = (
            database_events["trip_stop_hash"]
            .apply(binascii.hexlify)
            .str.decode("utf-8")
        )

    process_logger.add_metadata(db_event_count=database_events.shape[0])
    process_logger.log_complete()

    # merge all of the database data into the events we already have based on
    # trip stop hash. events that potentially need updated will have a pk_id
    # and potentially a vp_move_db or vp_stop_db element
    events = pandas.merge(
        events,
        database_events,
        how="left",
        on="trip_stop_hash",
    )

    # to update the vp move timestamp in the case where the db event had one
    # and the processed event did not. then update again in the case where the
    # prorcessed timestamp is later than the db timestamp.
    events["vp_move_timestamp"] = numpy.where(
        ((events["vp_move_db"].notna()) & (events["vp_move_timestamp"].isna())),
        events["vp_move_db"],
        events["vp_move_timestamp"],
    )
    events["vp_move_timestamp"] = numpy.where(
        (
            (events["vp_move_db"].notna())
            & (events["vp_move_timestamp"].notna())
            & (events["vp_move_timestamp"] > events["vp_move_db"])
        ),
        events["vp_move_db"],
        events["vp_move_timestamp"],
    )

    # same for vp_stop_timestamp
    events["vp_stop_timestamp"] = numpy.where(
        ((events["vp_stop_db"].notna()) & (events["vp_stop_timestamp"].isna())),
        events["vp_stop_db"],
        events["vp_stop_timestamp"],
    )
    events["vp_stop_timestamp"] = numpy.where(
        (
            (events["vp_stop_db"].notna())
            & (events["vp_stop_timestamp"].notna())
            & (events["vp_stop_timestamp"] > events["vp_stop_db"])
        ),
        events["vp_stop_db"],
        events["vp_stop_timestamp"],
    )

    # convert the hex hashes to bytes, convert timestamps and pk ids to ints
    events["trip_stop_hash"] = events["trip_stop_hash"].str.decode("hex")
    events["vp_move_timestamp"] = events["vp_move_timestamp"].astype("Int64")
    events["vp_stop_timestamp"] = events["vp_stop_timestamp"].astype("Int64")
    events["tu_stop_timestamp"] = events["tu_stop_timestamp"].astype("Int64")
    events["pk_id"] = events["pk_id"].astype("Int64")

    process_logger = ProcessLogger("gtfs_rt.update_events")
    process_logger.log_start()

    # update all of the events that have pk_ids and new timestamps.
    update_mask = (events["pk_id"].notna()) & (
        (events["tu_stop_timestamp"].notna())
        | (events["vp_move_timestamp"] != events["vp_move_db"])
        | (events["vp_stop_timestamp"] != events["vp_stop_db"])
    )

    # drop the db timestamps that have now been moved to the event timestamps
    # where appropriate and fill all nan's with nones for db insertion
    events = events.drop(columns=["vp_move_db", "vp_stop_db"])
    events = events.fillna(numpy.nan).replace([numpy.nan], [None])

    if update_mask.sum() > 0:
        update_cols = [
            "pk_id",
            "vp_move_timestamp",
            "vp_stop_timestamp",
            "tu_stop_timestamp",
        ]
        result = db_manager.execute_with_data(
            sa.update(VehicleEvents.__table__).where(
                VehicleEvents.pk_id == sa.bindparam("b_pk_id")
            ),
            events.loc[update_mask, update_cols].rename(
                columns={"pk_id": "b_pk_id"}
            ),
        )
        process_logger.add_metadata(db_update_rowcount=result.rowcount)

    process_logger.log_complete()
    process_logger = ProcessLogger("gtfs_rt.insert_events")
    process_logger.log_start()

    # any event that doesn't have a pk_id need to be inserted
    insert_mask = events["pk_id"].isna()

    process_logger.add_metadata(insert_event_count=insert_mask.sum())
    if insert_mask.sum() > 0:
        insert_cols = [
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
            "trip_hash",
            "stop_sequence",
            "stop_id",
            "parent_station",
            "trip_stop_hash",
            "vp_move_timestamp",
            "vp_stop_timestamp",
            "tu_stop_timestamp",
            "fk_static_timestamp",
        ]

        result = db_manager.execute_with_data(
            sa.insert(VehicleEvents.__table__),
            events.loc[insert_mask, insert_cols],
        )

    process_logger.log_complete()


def process_gtfs_rt_files(db_manager: DatabaseManager) -> None:
    """
    process vehicle position and trip update gtfs_rt files

    convert all of the entries in these files into vehicle events that
    represent unique trip and stop pairs. these events can have a vp_move_time
    (when the vehicle started moving towards the stop), a vp_stop_time (when
    the vehicle arrived at the stop as derrived from vehicle position data),
    and a tu_stop_time (when the vehicle arrived at the stop as derrived from
    the trip updates data).

    these vehicle events will then be combined with existing vehicle events in
    the database where we can compute travel time, dwell time, and headway
    metrics for each event.

    these events are either inserted into the Vehicle Events table or used to
    update existing rows in the table.

    finally, the MetadataLog table will be updated, marking the files as
    processed upon success. if a failure happens in processing, the failure
    will be logged and the file will be marked with a process_fail in the
    MetadataLog table.
    """
    hours_to_process = 6
    process_logger = ProcessLogger("l0_gtfs_rt_tables_loader")
    process_logger.log_start()

    for files in get_gtfs_rt_paths(db_manager):
        check_for_sigterm()
        if hours_to_process == 0:
            break
        hours_to_process -= 1

        subprocess_logger = ProcessLogger(
            "l0_gtfs_rt_table_loader",
            tu_file_count=len(files["tu_paths"]),
            vp_file_count=len(files["vp_paths"]),
        )
        subprocess_logger.log_start()

        try:
            if len(files["tu_paths"]) == 0:
                # all events come from vp files. add tu key afterwards.
                events = process_vp_files(files["vp_paths"], db_manager)
                events["tu_stop_timestamp"] = None
            elif len(files["vp_paths"]) == 0:
                # all events come from tu files. add vp keys afterwards.
                events = process_tu_files(files["tu_paths"], db_manager)
                events["vp_move_timestamp"] = None
                events["vp_stop_timestamp"] = None
            else:
                # events come from tu and vp files. join them together.
                vp_events = process_vp_files(files["vp_paths"], db_manager)
                tu_events = process_tu_files(files["tu_paths"], db_manager)
                events = combine_events(vp_events, tu_events)

            upload_to_database(events, db_manager)

            db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(files["ids"]))
                .values(processed=1)
            )
            subprocess_logger.add_metadata(event_count=events.shape[0])
            subprocess_logger.log_complete()
        except Exception as error:
            db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(files["ids"]))
                .values(processed=1, process_fail=1)
            )
            subprocess_logger.log_failure(error)

    process_logger.log_complete()
