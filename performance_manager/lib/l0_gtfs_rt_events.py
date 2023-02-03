from typing import Dict, List

import numpy
import pandas
import sqlalchemy as sa

from .l0_rt_trip_updates import process_tu_files
from .l0_rt_vehicle_positions import process_vp_files
from .logging_utils import ProcessLogger
from .postgres_schema import MetadataLog, TempHashCompare, VehicleEvents
from .postgres_utils import DatabaseManager, get_unprocessed_files
from .s3_utils import get_utc_from_partition_path


def get_gtfs_rt_paths(db_manager: DatabaseManager) -> List[Dict[str, List]]:
    """
    get all of the gtfs_rt files and group them by timestamp

    """
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

    return [
        grouped_files[timestamp] for timestamp in sorted(grouped_files.keys())
    ]


def collapse_events(
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
    # sort by trip stop hash and then tu stop timestamp
    events = pandas.concat([vp_events, tu_events])
    events = events.sort_values(by=["trip_stop_hash", "tu_stop_timestamp"])

    # shift the vp_stop_timestamp and the vp_move_timestamp columns up on
    # successive rows with the same trip stop hash
    events["vp_stop_timestamp"] = numpy.where(
        (events["trip_stop_hash"] == events["trip_stop_hash"].shift(-1)),
        events["vp_stop_timestamp"].shift(-1),
        events["vp_stop_timestamp"],
    )

    events["vp_move_timestamp"] = numpy.where(
        (events["trip_stop_hash"] == events["trip_stop_hash"].shift(-1)),
        events["vp_move_timestamp"].shift(-1),
        events["vp_move_timestamp"],
    )

    # drop the second of successive trip stop hash rows all of the important
    # data has been shifted up.
    return events.drop_duplicates(subset=["trip_stop_hash"], keep="first")


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
    # remove everything from the temp hash table and insert the trip stop hashs
    # from the new events. then pull events from the VehicleEvents table by
    # matching those hashes, which will be the events that will potentially be
    # updated.
    db_manager.truncate_table(TempHashCompare)
    db_manager.execute_with_data(
        sa.insert(TempHashCompare.__table__), events[["trip_stop_hash"]]
    )

    existing_events = db_manager.select_as_dataframe(
        sa.select(
            VehicleEvents.pk_id,
            VehicleEvents.trip_stop_hash,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleEvents.tu_stop_timestamp,
        ).join(
            TempHashCompare,
            TempHashCompare.trip_stop_hash == VehicleEvents.trip_stop_hash,
        )
    )

    # combine the existing vehicle events with the new events. sort them by
    # trip stop hash so that vehicle events from the same trip and stop will be
    # consecutive. the existing events will have a pk id while the new ones
    # will not. sorting those with na=last ensures they are ordered existing
    # first and new second
    all_events = pandas.concat([existing_events, events]).sort_values(
        by=["trip_stop_hash", "pk_id"], na_position="last"
    )

    # create a mask for duplicated events that will be used in the both the update
    # and insert masks
    duplicate_mask = all_events.duplicated(subset="trip_stop_hash", keep=False)

    # update events are more complicated. find trip stop pairs that are
    # duplicated, implying the came both from the gtfs_rt files and the
    # VehicleEvents table. next, only get the first ones that come from the
    # VehicleEvents table, where the pk_id is set. lastly, leave only the
    # events that need any of their vp_move, vp_stop, or tu_stop times
    # updated.
    update_mask = (
        (duplicate_mask)
        & (all_events["pk_id"].notna())
        & (
            (
                (~all_events["vp_move_timestamp"].shift(-1).isna())
                & (
                    (all_events["vp_move_timestamp"].isna())
                    | (
                        all_events["vp_move_timestamp"]
                        > all_events["vp_move_timestamp"].shift(-1)
                    )
                )
            )
            | (
                (~all_events["vp_stop_timestamp"].shift(-1).isna())
                & (
                    (all_events["vp_stop_timestamp"].isna())
                    | (
                        all_events["vp_stop_timestamp"]
                        > all_events["vp_stop_timestamp"].shift(-1)
                    )
                )
            )
            | (
                (~all_events["tu_stop_timestamp"].shift(-1).isna())
                & (
                    (all_events["tu_stop_timestamp"].isna())
                    | (
                        all_events["tu_stop_timestamp"]
                        > all_events["tu_stop_timestamp"].shift(-1)
                    )
                )
            )
        )
    )

    if update_mask.sum() > 0:
        # get all of the events that have a trip stop hash that needs to be
        # updated
        update_hashes = all_events.loc[update_mask, "trip_stop_hash"]
        update_events = all_events[
            all_events["trip_stop_hash"].isin(update_hashes)
        ]

        # update the vp move, vp stop, and tu stop times. its the same logic
        # for all of them. if the trip stop hash is the same in consecutive
        # events, the first is from the VehicleEvents table and the second is
        # from the gtfs_rt files. if the existing event doesn't have a time
        # or the existing time is later than the newly processed one, then
        # use the new time. otherwise keep the old one.
        update_events["vp_move_timestamp"] = numpy.where(
            (
                update_events["trip_stop_hash"]
                == update_events["trip_stop_hash"].shift(-1)
            )
            & (
                (
                    update_events["vp_move_timestamp"]
                    > update_events["vp_move_timestamp"].shift(-1)
                )
                | (update_events["vp_move_timestamp"].isna())
            ),
            update_events["vp_move_timestamp"].shift(-1),
            update_events["vp_move_timestamp"],
        )

        update_events["vp_stop_timestamp"] = numpy.where(
            (
                update_events["trip_stop_hash"]
                == update_events["trip_stop_hash"].shift(-1)
            )
            & (
                (
                    update_events["vp_stop_timestamp"]
                    > update_events["vp_stop_timestamp"].shift(-1)
                )
                | (update_events["vp_stop_timestamp"].isna())
            ),
            update_events["vp_stop_timestamp"].shift(-1),
            update_events["vp_stop_timestamp"],
        )

        update_events["tu_stop_timestamp"] = numpy.where(
            (
                update_events["trip_stop_hash"]
                == update_events["trip_stop_hash"].shift(-1)
            )
            & (
                (
                    update_events["tu_stop_timestamp"]
                    > update_events["tu_stop_timestamp"].shift(-1)
                )
                | (update_events["tu_stop_timestamp"].isna())
            ),
            update_events["tu_stop_timestamp"].shift(-1),
            update_events["tu_stop_timestamp"],
        )

        # drop all events without a pk_id. the data in them has been copied
        # into the other events if appropriate.
        update_events = update_events[update_events["pk_id"].notna()]

        update_cols = [
            "pk_id",
            "vp_move_timestamp",
            "vp_stop_timestamp",
            "tu_stop_timestamp",
        ]
        db_manager.execute_with_data(
            sa.update(VehicleEvents.__table__).where(
                VehicleEvents.pk_id == sa.bindparam("b_pk_id")
            ),
            update_events[update_cols].rename(columns={"pk_id": "b_pk_id"}),
        )

    # events that aren't duplicated came exclusively from the gtfs_rt files or the
    # VehicleEvents table. filter out events without pk_id's to remove events from
    # the VehicleEvents table.
    insert_mask = ~duplicate_mask & all_events["pk_id"].isna()

    if insert_mask.sum() > 0:
        insert_cols = [
            "direction_id",
            "route_id",
            "start_date",
            "start_time",
            "vehicle_id",
            "stop_sequence",
            "stop_id",
            "parent_station",
            "trip_stop_hash",
            "vp_move_timestamp",
            "vp_stop_timestamp",
            # "tu_stop_timestamp",
            "fk_static_timestamp",
        ]

        insert_events = all_events.loc[insert_mask, insert_cols]

        print(insert_events.dtypes)

        db_manager.execute_with_data(
            sa.insert(VehicleEvents.__table__), insert_events
        )


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
    process_logger = ProcessLogger("l0_tables_loader")
    process_logger.log_start()

    for files in get_gtfs_rt_paths(db_manager):
        if hours_to_process == 0:
            break
        hours_to_process -= 1

        subprocess_logger = ProcessLogger(
            "l0_table_loader",
            tu_file_count=len(files["tu_paths"]),
            vp_file_count=len(files["vp_paths"]),
        )
        subprocess_logger.log_start()

        try:
            vp_events = process_vp_files(files["vp_paths"], db_manager)
            tu_events = process_tu_files(files["tu_paths"], db_manager)

            events = collapse_events(vp_events, tu_events)
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
