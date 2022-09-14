from dataclasses import dataclass
from typing import Dict
import pandas
import numpy

import sqlalchemy as sa

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    TripUpdateEvents,
    FullTripEvents,
    TempFullTripEvents,
)
from .gtfs_utils import add_event_hash_column


@dataclass
class EventsToMerge:
    """
    container class for processing events that will be merged to make trip events
    """

    select_query: sa.sql.selectable.Select
    fk_rename_field: str
    merge_dataframe: pandas.DataFrame = pandas.DataFrame()


def get_merge_list() -> Dict[str, EventsToMerge]:
    """
    getter function for dict of merge event objects
    """
    process_logger = ProcessLogger("l1_get_merge_list")
    process_logger.log_start()

    # query to remove "error" records from events going into FullTripEvents
    # no records should have duplicate hash values unlesss something strange has
    # happened in the gtfs-rt data stream
    dupe_hash_query = (
        sa.select(VehiclePositionEvents.hash)
        .group_by(VehiclePositionEvents.hash)
        .having(sa.func.count() > 1)
    )

    moving_vp_events = EventsToMerge(
        select_query=sa.select(
            VehiclePositionEvents.pk_id,
            VehiclePositionEvents.stop_sequence,
            VehiclePositionEvents.stop_id,
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        ).where(
            (VehiclePositionEvents.is_moving == sa.true())
            & (
                VehiclePositionEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_vp_moving_event).where(
                        FullTripEvents.fk_vp_moving_event.isnot(None)
                    )
                )
            )
            & (VehiclePositionEvents.hash.notin_(dupe_hash_query))
        ),
        fk_rename_field="fk_vp_moving_event",
    )

    stopped_vp_events = EventsToMerge(
        select_query=sa.select(
            VehiclePositionEvents.pk_id,
            VehiclePositionEvents.stop_sequence,
            VehiclePositionEvents.stop_id,
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        ).where(
            (VehiclePositionEvents.is_moving == sa.false())
            & (
                VehiclePositionEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_vp_stopped_event).where(
                        FullTripEvents.fk_vp_stopped_event.isnot(None)
                    )
                )
            )
            & (VehiclePositionEvents.hash.notin_(dupe_hash_query))
        ),
        fk_rename_field="fk_vp_stopped_event",
    )

    stopped_tu_events = EventsToMerge(
        select_query=sa.select(
            TripUpdateEvents.pk_id,
            TripUpdateEvents.stop_sequence,
            TripUpdateEvents.stop_id,
            TripUpdateEvents.direction_id,
            TripUpdateEvents.route_id,
            TripUpdateEvents.start_date,
            TripUpdateEvents.start_time,
            TripUpdateEvents.vehicle_id,
        ).where(
            (
                TripUpdateEvents.pk_id.notin_(
                    sa.select(FullTripEvents.fk_tu_stopped_event).where(
                        FullTripEvents.fk_tu_stopped_event.isnot(None)
                    )
                )
            )
        ),
        fk_rename_field="fk_tu_stopped_event",
    )

    process_logger.log_complete()

    return {
        "moving_vp": moving_vp_events,
        "stopped_vp": stopped_vp_events,
        "stopped_tu": stopped_tu_events,
    }


def pull_and_transform(
    events_list: Dict[str, EventsToMerge], db_manager: DatabaseManager
) -> None:
    """
    pull new events from events tables and transform
    """
    # columns to hash for FullTripEvents primary key field
    expected_hash_columns = [
        "stop_sequence",
        "stop_id",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]

    # these columns are converted to pandas Int64 type to ensure consistent hash
    convert_to_number_columns = [
        "stop_sequence",
        "direction_id",
        "start_date",
        "start_time",
    ]

    process_logger = ProcessLogger("l1_pull_events")
    process_logger.log_start()

    for event_type in events_list.values():
        # get dataframe from L0 table select
        event_type.merge_dataframe = db_manager.select_as_dataframe(
            event_type.select_query
        )

        # if L0 select produces no results, create empty frame with expected
        # columns "pk_id" & "hash"
        if event_type.merge_dataframe.shape[0] == 0:
            event_type.merge_dataframe = pandas.DataFrame(
                columns=["pk_id", "hash"]
            )
        else:
            # if data is retrieved from L0 table, ensure number columns are
            # Int64 type to ensure consistent hashing
            for column in convert_to_number_columns:
                event_type.merge_dataframe[column] = pandas.to_numeric(
                    event_type.merge_dataframe[column]
                ).astype("Int64")
            # create hash column to be used as FullTripEvent primary key
            # drop category columns after hash has been created
            event_type.merge_dataframe = add_event_hash_column(
                event_type.merge_dataframe,
                expected_hash_columns=expected_hash_columns,
            )[["pk_id", "hash"]]

        # rename pk_id field from L0 table to L1 foreign key field name
        event_type.merge_dataframe.rename(
            columns={"pk_id": event_type.fk_rename_field}, inplace=True
        )

    process_logger.log_complete()


def merge_events(events_list: Dict[str, EventsToMerge]) -> pandas.DataFrame:
    """
    merge all unprocessed event types into single dataframe
    """
    process_logger = ProcessLogger("l1_merge_events")
    process_logger.log_start()

    # left merge TRIP_UPDATE stop events with moving VEHICLE_POSITION events
    # TRIP_UPDATE predicted stop times are only helpful as supplement to
    # VEHICLE_POSITION moving event
    return_dataframe = events_list["moving_vp"].merge_dataframe.merge(
        events_list["stopped_tu"].merge_dataframe, how="left", on="hash"
    )
    # outer join VEHICLE_POSITION stop events to capture all recorded stop
    # events, even if they do not have associated moving event
    return_dataframe = return_dataframe.merge(
        events_list["stopped_vp"].merge_dataframe, how="outer", on="hash"
    )

    # merge action turns foreign key field into "float" type, this will
    # convert foreign keys back to integer for update / insert action
    for column in return_dataframe.columns:
        if column == "hash":
            continue
        return_dataframe[column] = return_dataframe[column].astype("Int64")

    # make sure all "na" types are replace as Python None for RDS
    # insert / upate
    return_dataframe = return_dataframe.fillna(numpy.nan).replace(
        [numpy.nan], [None]
    )

    process_logger.log_complete()
    return return_dataframe


def load_temp_table(
    insert_dataframe: pandas.DataFrame, db_manager: DatabaseManager
) -> None:
    """
    truncate temp loading table TempFullTripEvents and insert new
    trip event records for update / insert with FullTripEvents table
    """
    db_manager.truncate_table(TempFullTripEvents)

    db_manager.insert_dataframe(insert_dataframe, TempFullTripEvents.__table__)


def update_with_new_events(
    insert_dataframe: pandas.DataFrame, db_manager: DatabaseManager
) -> None:
    """
    update FullTripEvents table if insert_dataframe has overlapping hash values

    this can normally be done with and sql UPDATE-FROM query, but sqlalchemy
    does not support that type of query with sqlite dev database

    alternate approach is used here where just hashes are selected that represent
    update records and each column is updated seperatly from insert_dataframe
    """
    process_logger = ProcessLogger("l1_update_events")
    process_logger.log_start()

    # get dataframe of hashes represnting records in FullTripEvent table that
    # need to be update from loading TempFullTripEvents table
    hash_to_update_query = sa.select(TempFullTripEvents.hash).where(
        TempFullTripEvents.hash.in_(sa.select(FullTripEvents.hash))
    )
    hashes_to_update = db_manager.select_as_dataframe(hash_to_update_query)

    process_logger.add_metadata(trip_events_to_update=hashes_to_update.shape[0])

    # gate to see if any records need updating
    if hashes_to_update.shape[0] == 0:
        process_logger.log_complete()
        return

    # mask to select records in insert_dataframe that are part of update
    to_update_mask = insert_dataframe["hash"].isin(hashes_to_update["hash"])

    # update query to be used for all FullTripEvents updates "hash" columns wil
    # be renamed to "b_hash" because "hash" is protected in sqlalchemy
    update_query = sa.update(FullTripEvents.__table__).where(
        FullTripEvents.hash == sa.bindparam("b_hash")
    )

    # mask for insert dataframe and vp_moving events
    vp_moving_mask = (~insert_dataframe["fk_vp_moving_event"].isna()) & (
        to_update_mask
    )
    # if mask shows records need updating, execute update
    if vp_moving_mask.sum() > 0:
        result = db_manager.execute_with_data(
            update_query,
            insert_dataframe.loc[
                vp_moving_mask, ["hash", "fk_vp_moving_event"]
            ].rename(columns={"hash": "b_hash"}),
        )
        process_logger.add_metadata(fk_vp_moving_events_updated=result.rowcount)
    else:
        process_logger.add_metadata(fk_vp_moving_events_updated=0)

    # mask for insert dataframe and vp_stopped events
    vp_stopped_mask = (~insert_dataframe["fk_vp_stopped_event"].isna()) & (
        to_update_mask
    )
    # if mask shows records need updating, execute update
    if vp_stopped_mask.sum() > 0:
        result = db_manager.execute_with_data(
            update_query,
            insert_dataframe.loc[
                vp_stopped_mask, ["hash", "fk_vp_stopped_event"]
            ].rename(columns={"hash": "b_hash"}),
        )
        process_logger.add_metadata(
            fk_vp_stopped_events_updated=result.rowcount
        )
    else:
        process_logger.add_metadata(fk_vp_stopped_events_updated=0)

    # mask for insert dataframe and tu_stopped events
    tu_stopped_mask = (~insert_dataframe["fk_tu_stopped_event"].isna()) & (
        to_update_mask
    )
    # if mask shows records need updating, execute update
    if tu_stopped_mask.sum() > 0:
        result = db_manager.execute_with_data(
            update_query,
            insert_dataframe.loc[
                tu_stopped_mask, ["hash", "fk_tu_stopped_event"]
            ].rename(columns={"hash": "b_hash"}),
        )
        process_logger.add_metadata(
            fk_tu_stopped_events_updated=result.rowcount
        )
    else:
        process_logger.add_metadata(fk_tu_stopped_events_updated=0)

    process_logger.log_complete()


def insert_new_events(db_manager: DatabaseManager) -> None:
    """
    insert new events into FullTripEvents table with INSERT from SELECT operation
    table to table insertion
    """
    insert_cols = [
        "hash",
        "fk_vp_stopped_event",
        "fk_vp_moving_event",
        "fk_tu_stopped_event",
    ]

    process_logger = ProcessLogger("l1_insert_events")
    process_logger.log_start()
    insert_query = (
        FullTripEvents.metadata.tables[FullTripEvents.__tablename__]
        .insert()
        .from_select(
            insert_cols,
            sa.select(
                TempFullTripEvents.hash,
                TempFullTripEvents.fk_vp_stopped_event,
                TempFullTripEvents.fk_vp_moving_event,
                TempFullTripEvents.fk_tu_stopped_event,
            ).where(
                (TempFullTripEvents.hash.notin_(sa.select(FullTripEvents.hash)))
            ),
        )
    )
    result = db_manager.execute(insert_query)

    process_logger.add_metadata(rows_inserted=result.rowcount)
    process_logger.log_complete()


def process_full_trip_events(db_manager: DatabaseManager) -> None:
    """
    process new events from L0 tables and insert to L1 FullTripEvents RDS table

    primary key and category columns will be selected from 3 data sources:
        - RT_VEHICLE_POSITIONS (moving events)
        - RT_VEHICLE_POSITIONS (stopped events)
        - TRIP_UPDATES (stopped events)

    primary keys from l0 tables are selected if they do not exist in their
    corresponding foreign key column in FullTripEvents table

    TRIP_UPDATE events are predictions used to backfill stop events for moving
    events that had no associated stopped event (mostly commuter rail)

    category columns from the 3 sources are hashed in the same manner and
    events are matched together based on hashes, these hashes will function
    as the primary key in the FullTripEvents table

    all valid RT_VEHICLE_POSITIONS (moving & stopped) events are merged to create
    records in the FullTRipEvents table (using outer join), so stop events may
    exist without moving events and vice versa

    TRIP_UPDATE stop events are only merged to a valid RT_VEHICLE_POSITIONS
    moving event (using left join)

    all merged new FullTripEvents records are inserted into temporary loading
    table (TempFullTripEvents)

    if computed hashes in TempFullTripEvents already exist in FullTripEvents
    table, foreign key fields are updated with non-null key values

    if computed hahes in TempFullTripEvents DO NOT exist in FullTripEvents,
    full records are inserted from TempFullTripEvents into FullTripEvents
    """
    process_logger = ProcessLogger("process_l1_events")
    process_logger.log_start()
    try:
        events_list = get_merge_list()
        pull_and_transform(events_list, db_manager)
        insert_dataframe = merge_events(events_list)
        process_logger.add_metadata(
            new_full_trip_records=insert_dataframe.shape[0]
        )

        # gate to check for no records needing update / insert
        if insert_dataframe.shape[0] == 0:
            process_logger.log_complete()
            return

        load_temp_table(insert_dataframe, db_manager)
        update_with_new_events(insert_dataframe, db_manager)
        insert_new_events(db_manager)
        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)
