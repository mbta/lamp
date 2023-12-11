from typing import Dict, List, Tuple

import numpy
import pandas
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from lamp_py.aws.ecs import check_for_sigterm
from lamp_py.aws.s3 import get_datetime_from_partition_path
from lamp_py.postgres.metadata_schema import MetadataLog
from lamp_py.postgres.rail_performance_manager_schema import (
    TempEventCompare,
    VehicleEvents,
    VehicleTrips,
)
from lamp_py.postgres.postgres_utils import (
    DatabaseManager,
    get_unprocessed_files,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_utils import unique_trip_stop_columns
from .l0_rt_trip_updates import process_tu_files
from .l0_rt_vehicle_positions import process_vp_files
from .l1_rt_trips import process_trips, load_new_trip_data
from .l1_rt_metrics import update_metrics_from_temp_events


def get_gtfs_rt_paths(md_db_manager: DatabaseManager) -> List[Dict[str, List]]:
    """
    get all of the gtfs_rt files and group them by the hour they are from.
    within a group, include the non bus route ids for the timestamp
    """
    process_logger = ProcessLogger("gtfs_rt.get_files")
    process_logger.log_start()

    grouped_files = {}

    vp_files = get_unprocessed_files("RT_VEHICLE_POSITIONS", md_db_manager)
    for record in vp_files:
        timestamp = get_datetime_from_partition_path(
            record["paths"][0]
        ).timestamp()

        grouped_files[timestamp] = {
            "ids": record["ids"],
            "vp_paths": record["paths"],
            "tu_paths": [],
        }

    tu_files = get_unprocessed_files("RT_TRIP_UPDATES", md_db_manager)
    for record in tu_files:
        timestamp = get_datetime_from_partition_path(
            record["paths"][0]
        ).timestamp()
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
    tu_stop_time entry.

    join the dataframes and collapse rows representing the same trip and
    stop pairs.
    """
    process_logger = ProcessLogger(
        "gtfs_rt.combine_events",
        vp_event_count=vp_events.shape[0],
        tu_event_count=tu_events.shape[0],
    )
    process_logger.log_start()

    trip_stop_columns = unique_trip_stop_columns()

    # merge together the trip_stop_columns and the timestamps
    events = pandas.merge(
        vp_events[
            trip_stop_columns + ["vp_stop_timestamp", "vp_move_timestamp"]
        ],
        tu_events[
            trip_stop_columns
            + [
                "tu_stop_timestamp",
            ]
        ],
        how="outer",
        on=trip_stop_columns,
        validate="one_to_one",
    )

    # concat all of the details that went into each trip event, dropping
    # duplicates.
    # DRAGONS
    # selection of unique `details_columns` records could have non-deterministic behavior
    #
    # many vehicle position and trip update events have to be aggregated together
    # for each unique trip-stop event record. `details_columns` columns that are
    # not a part of `trip_stop_columns` are semi-randomly dropped, with the first
    # column being kept based on a sort-order.
    #
    # currently, the sort-order only takes into account NA values in a few
    # columns to priortize the selection of records from vehicle positions over
    # trip updates. beyond that, records are essentially dropped at randmon
    details_columns = [
        "service_date",
        "start_time",
        "route_id",
        "direction_id",
        "parent_station",
        "vehicle_id",
        "stop_id",
        "stop_sequence",
        "trip_id",
        "vehicle_label",
        "vehicle_consist",
        "static_version_key",
    ]

    event_details = pandas.concat(
        [vp_events[details_columns], tu_events[details_columns]]
    )

    # create sort column to indicate which records have null values for select columns
    # we want to drop these null value records whenever possible
    # to prioritize records from vehicle_positions
    event_details["na_sort"] = (
        event_details[["stop_sequence", "vehicle_label", "vehicle_consist"]]
        .isna()
        .sum(axis=1)
    )

    event_details = (
        event_details.sort_values(
            by=trip_stop_columns
            + [
                "na_sort",
            ],
            ascending=True,
        )
        .drop_duplicates(subset=trip_stop_columns, keep="first")
        .drop(columns="na_sort")
    )

    # join `details_columns` to df with timestamps
    events = events.merge(
        event_details, how="left", on=trip_stop_columns, validate="one_to_one"
    )

    process_logger.add_metadata(total_event_count=events.shape[0])
    process_logger.log_complete()

    return events


def flag_insert_update_events(db_manager: DatabaseManager) -> Tuple[int, int]:
    """
    update do_update and do_insert flag columns in temp_event_compare table

    delete records in temp_event_compare not related to update/insert operations

    get records counts for each flag an return

    @db_manager DatabaseManager object for database interaction

    @return Tuple[int, int] - do_update count, do_insert count
    """

    # populate do_update column of temp_event_compare
    update_do_update = (
        sa.update(TempEventCompare.__table__)
        .values(
            do_update=True,
        )
        .where(
            VehicleEvents.service_date == TempEventCompare.service_date,
            VehicleEvents.pm_trip_id == TempEventCompare.pm_trip_id,
            VehicleEvents.parent_station == TempEventCompare.parent_station,
            sa.or_(
                sa.and_(
                    TempEventCompare.vp_move_timestamp.is_not(None),
                    sa.or_(
                        VehicleEvents.vp_move_timestamp.is_(None),
                        VehicleEvents.vp_move_timestamp
                        > TempEventCompare.vp_move_timestamp,
                    ),
                ),
                sa.and_(
                    TempEventCompare.vp_stop_timestamp.is_not(None),
                    sa.or_(
                        VehicleEvents.vp_stop_timestamp.is_(None),
                        VehicleEvents.vp_stop_timestamp
                        > TempEventCompare.vp_move_timestamp,
                    ),
                ),
                TempEventCompare.tu_stop_timestamp.is_not(None),
            ),
        )
    )
    db_manager.execute(update_do_update)

    # get count of do_update records
    update_count_query = sa.select(
        sa.func.count(TempEventCompare.do_update)
    ).where(TempEventCompare.do_update == sa.true())
    update_count = int(
        db_manager.select_as_list(update_count_query)[0]["count"]
    )

    # populate do_insert column of temp_event_compare
    do_insert_pre_select = (
        sa.select(None)
        .where(
            VehicleEvents.service_date == TempEventCompare.service_date,
            VehicleEvents.pm_trip_id == TempEventCompare.pm_trip_id,
            VehicleEvents.parent_station == TempEventCompare.parent_station,
        )
        .exists()
    )
    update_do_insert = (
        sa.update(TempEventCompare.__table__)
        .values(
            do_insert=True,
        )
        .where(~do_insert_pre_select)
    )
    db_manager.execute(update_do_insert)

    # get count of do_insert records
    insert_count_query = sa.select(
        sa.func.count(TempEventCompare.do_insert)
    ).where(TempEventCompare.do_insert == sa.true())
    insert_count = int(
        db_manager.select_as_list(insert_count_query)[0]["count"]
    )

    # remove records from temp_event_compare that are not related to updates or inserts
    delete_temp = sa.delete(TempEventCompare.__table__).where(
        TempEventCompare.do_update == sa.false(),
        TempEventCompare.do_insert == sa.false(),
    )
    db_manager.execute(delete_temp)

    return (update_count, insert_count)


def build_temp_events(
    events: pandas.DataFrame, db_manager: DatabaseManager
) -> pandas.DataFrame:
    """
    add vehicle event data to the database

    pull existing events from the database that overlap with the proccessed
    events. split the processed events into those whose trip_stop_hash is
    already in the database and those that are brand new. update preexisting
    events where appropriate and insert the new ones.
    """
    process_logger = ProcessLogger(
        "gtfs_rt.build_temp_events",
        event_count=events.shape[0],
    )
    process_logger.log_start()

    events["vp_move_timestamp"] = events["vp_move_timestamp"].astype("Int64")
    events["vp_stop_timestamp"] = events["vp_stop_timestamp"].astype("Int64")
    events["tu_stop_timestamp"] = events["tu_stop_timestamp"].astype("Int64")
    events = events.fillna(numpy.nan).replace([numpy.nan], [None])

    # truncate temp_event_compare table and insert all event records
    db_manager.truncate_table(TempEventCompare)
    db_manager.execute_with_data(
        sa.insert(TempEventCompare.__table__),
        events,
    )

    # make sure vehicle_trips has trips for all events in temp_event_compare
    load_new_trip_data(db_manager=db_manager)

    # load pm_trip_id values into temp_event_compare from vehicle_trips
    update_temp_trip_id = (
        sa.update(TempEventCompare.__table__)
        .values(pm_trip_id=VehicleTrips.pm_trip_id)
        .where(
            TempEventCompare.service_date == VehicleTrips.service_date,
            TempEventCompare.route_id == VehicleTrips.route_id,
            TempEventCompare.trip_id == VehicleTrips.trip_id,
        )
    )
    db_manager.execute(update_temp_trip_id)

    # populate do_insert and do_update columns of temp_event_compare
    # get counts of records belonging to each flag
    update_count, insert_count = flag_insert_update_events(db_manager)
    process_logger.add_metadata(
        db_update_rowcount=update_count,
        db_insert_rowcount=insert_count,
    )

    process_logger.log_complete()

    return update_count + insert_count


def update_events_from_temp(db_manager: DatabaseManager) -> None:
    """ "
    Insert and Update vehicle_events table from records in temp_event_compare

    """
    process_logger = ProcessLogger(
        "gtfs_rt.insert_update_events",
    )
    process_logger.log_start()

    # INSERT/UPDATE vehicle_events records with vp_move_timestamp
    # this will insert new event records into vehicle_events if they DO NOT exist
    # if they DO exist columns will be updated if necessary
    select_vp_move = sa.select(
        TempEventCompare.service_date,
        TempEventCompare.pm_trip_id,
        TempEventCompare.stop_sequence,
        TempEventCompare.stop_id,
        TempEventCompare.parent_station,
        TempEventCompare.vp_move_timestamp,
    )
    insert_columns = [
        "service_date",
        "pm_trip_id",
        "stop_sequence",
        "stop_id",
        "parent_station",
        "vp_move_timestamp",
    ]
    insert_update_vp_move_events = (
        postgresql.insert(VehicleEvents.__table__)
        .from_select(
            insert_columns,
            select_vp_move,
        )
        .on_conflict_do_update(
            index_elements=[
                VehicleEvents.service_date,
                VehicleEvents.pm_trip_id,
                VehicleEvents.parent_station,
            ],
            set_={
                "vp_move_timestamp": sa.text("excluded.vp_move_timestamp"),
                "stop_sequence": sa.text("excluded.stop_sequence"),
                "stop_id": sa.text("excluded.stop_id"),
            },
            where=sa.and_(
                sa.text("excluded.vp_move_timestamp IS NOT NULL"),
                sa.or_(
                    VehicleEvents.vp_move_timestamp.is_(None),
                    VehicleEvents.vp_move_timestamp
                    > sa.text("excluded.vp_move_timestamp"),
                ),
            ),
        )
    )
    db_manager.execute(insert_update_vp_move_events)

    # update vehicle_events records with vp_stop_timestamp, if required
    update_vp_stop_events = (
        sa.update(VehicleEvents.__table__)
        .values(
            vp_stop_timestamp=TempEventCompare.vp_stop_timestamp,
            stop_sequence=TempEventCompare.stop_sequence,
            stop_id=TempEventCompare.stop_id,
        )
        .where(
            VehicleEvents.service_date == TempEventCompare.service_date,
            VehicleEvents.pm_trip_id == TempEventCompare.pm_trip_id,
            VehicleEvents.parent_station == TempEventCompare.parent_station,
            TempEventCompare.vp_stop_timestamp.is_not(None),
            sa.or_(
                VehicleEvents.vp_stop_timestamp.is_(None),
                VehicleEvents.vp_stop_timestamp
                > TempEventCompare.vp_stop_timestamp,
            ),
        )
    )
    db_manager.execute(update_vp_stop_events)

    # update vehicle_events records with tu_stop_timestamp
    #
    # vehicle_events > temp_event_compare comparison is not used for trip updates
    # we don't just want the earliest timestamp value for trip updates, we want the
    # last value before the trip arrives at the parent_station
    # so, we assume trip updates are processed chronologically and always
    # insert the last trip update timestamp value that is recieved
    update_tu_stop_events = (
        sa.update(VehicleEvents.__table__)
        .values(
            tu_stop_timestamp=TempEventCompare.tu_stop_timestamp,
        )
        .where(
            VehicleEvents.service_date == TempEventCompare.service_date,
            VehicleEvents.pm_trip_id == TempEventCompare.pm_trip_id,
            VehicleEvents.parent_station == TempEventCompare.parent_station,
            TempEventCompare.tu_stop_timestamp.is_not(None),
        )
    )
    db_manager.execute(update_tu_stop_events)

    process_logger.log_complete()


def process_gtfs_rt_files(
    rpm_db_manager: DatabaseManager,
    md_db_manager: DatabaseManager,
) -> None:
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

    for files in get_gtfs_rt_paths(md_db_manager):
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
                # only vp files available. create dummy tu events frame.
                vp_events = process_vp_files(
                    paths=files["vp_paths"],
                    db_manager=rpm_db_manager,
                )
                tu_events = pandas.DataFrame()
            elif len(files["vp_paths"]) == 0:
                # only tu files available. create dummy vp events frame.
                tu_events = process_tu_files(
                    paths=files["tu_paths"],
                    db_manager=rpm_db_manager,
                )
                vp_events = pandas.DataFrame()
            else:
                # files available for vp and tu events
                vp_events = process_vp_files(
                    paths=files["vp_paths"],
                    db_manager=rpm_db_manager,
                )
                tu_events = process_tu_files(
                    paths=files["tu_paths"],
                    db_manager=rpm_db_manager,
                )

            if vp_events.shape[0] > 0 and tu_events.shape[0] > 0:
                # combine events when available from tu and vp
                events = combine_events(vp_events, tu_events)
            elif vp_events.shape[0] == 0 and tu_events.shape[0] == 0:
                # no events found
                events = pandas.DataFrame()
            elif tu_events.shape[0] == 0:
                # no tu events found
                events = vp_events
                events["tu_stop_timestamp"] = None
            elif vp_events.shape[0] == 0:
                # no vp events found
                events = tu_events
                events["vp_move_timestamp"] = None
                events["vp_stop_timestamp"] = None

            # continue events processing if records exist
            if events.shape[0] > 0:
                change_count = build_temp_events(events, rpm_db_manager)

                if change_count > 0:
                    update_events_from_temp(rpm_db_manager)
                    # update trips data in vehicle_trips table
                    process_trips(rpm_db_manager)
                    # update event metrics columns
                    update_metrics_from_temp_events(rpm_db_manager)

            md_db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(files["ids"]))
                .values(rail_pm_processed=True)
            )
            subprocess_logger.add_metadata(event_count=events.shape[0])
            subprocess_logger.log_complete()
        except Exception as error:
            md_db_manager.execute(
                sa.update(MetadataLog.__table__)
                .where(MetadataLog.pk_id.in_(files["ids"]))
                .values(rail_pm_processed=True, rail_pm_process_fail=True)
            )
            subprocess_logger.log_failure(error)

    rpm_db_manager.vacuum_analyze(VehicleEvents)
    rpm_db_manager.vacuum_analyze(VehicleTrips)

    process_logger.log_complete()
