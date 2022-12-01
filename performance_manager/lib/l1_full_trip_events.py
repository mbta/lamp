import pandas

import sqlalchemy as sa

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    TripUpdateEvents,
    FullTripEvents,
    TempFullTripEvents,
    StaticStops,
)
from .gtfs_utils import add_event_hash_column


def pull_and_transform(db_manager: DatabaseManager) -> pandas.DataFrame:
    """
    pull new events from events tables and transform
    """
    # columns to hash for FullTripEvents primary key field
    expected_hash_columns = [
        "stop_sequence",
        "parent_station",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "vehicle_id",
    ]

    # these columns are converted to pandas Int64 type to ensure consistent hash
    convert_to_number_columns = [
        "fk_vp_moving_event",
        "fk_vp_stopped_event",
        "fk_tu_stopped_event",
        "stop_sequence",
        "direction_id",
        "start_date",
        "start_time",
    ]

    merged_columns_to_keep = [
        "fk_vp_moving_event",
        "fk_vp_stopped_event",
        "fk_tu_stopped_event",
        "hash",
    ]

    last_full_event_update_query = (
        sa.select(sa.func.coalesce(
            sa.func.max(FullTripEvents.updated_on),
            sa.func.to_timestamp(0),
        ))
    ).scalar_subquery()

    dupe_hash_cte = (
        sa.select(VehiclePositionEvents.hash)
        .group_by(VehiclePositionEvents.hash)
        .having(sa.func.count() > 1)
    ).cte("dupe_hash")

    move_vp_cte = (
        sa.select(
            VehiclePositionEvents.pk_id.label("fk_vp_moving_event"),
            VehiclePositionEvents.stop_sequence,
            sa.case(
                (
                    StaticStops.parent_station.is_(None),
                    VehiclePositionEvents.stop_id,
                ),
                else_=StaticStops.parent_station,
            ).label("parent_station"),
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        )
        .join(
            dupe_hash_cte,
            VehiclePositionEvents.hash == dupe_hash_cte.c.hash,
            isouter=True,
        )
        .join(
            StaticStops,
            sa.and_(
                VehiclePositionEvents.fk_static_timestamp
                == StaticStops.timestamp,
                VehiclePositionEvents.stop_id == StaticStops.stop_id,
            ),
            isouter=True,
        )
        .where(
            (VehiclePositionEvents.updated_on > last_full_event_update_query)
            & (VehiclePositionEvents.is_moving == sa.true())
            & (VehiclePositionEvents.stop_sequence.isnot(None))
            & (VehiclePositionEvents.stop_id.isnot(None))
            & (VehiclePositionEvents.direction_id.isnot(None))
            & (VehiclePositionEvents.route_id.isnot(None))
            & (VehiclePositionEvents.start_date.isnot(None))
            & (VehiclePositionEvents.start_time.isnot(None))
            & (VehiclePositionEvents.vehicle_id.isnot(None))
            & (dupe_hash_cte.c.hash.is_(None))
        )
        .cte(name="move_vp")
    )

    stop_vp_cte = (
        sa.select(
            VehiclePositionEvents.pk_id.label("fk_vp_stopped_event"),
            VehiclePositionEvents.stop_sequence,
            sa.case(
                (
                    StaticStops.parent_station.is_(None),
                    VehiclePositionEvents.stop_id,
                ),
                else_=StaticStops.parent_station,
            ).label("parent_station"),
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.start_time,
            VehiclePositionEvents.vehicle_id,
        )
        .join(
            dupe_hash_cte,
            VehiclePositionEvents.hash == dupe_hash_cte.c.hash,
            isouter=True,
        )
        .join(
            StaticStops,
            sa.and_(
                VehiclePositionEvents.fk_static_timestamp
                == StaticStops.timestamp,
                VehiclePositionEvents.stop_id == StaticStops.stop_id,
            ),
            isouter=True,
        )
        .where(
            (VehiclePositionEvents.updated_on > last_full_event_update_query)
            & (VehiclePositionEvents.is_moving == sa.false())
            & (VehiclePositionEvents.stop_sequence.isnot(None))
            & (VehiclePositionEvents.stop_id.isnot(None))
            & (VehiclePositionEvents.direction_id.isnot(None))
            & (VehiclePositionEvents.route_id.isnot(None))
            & (VehiclePositionEvents.start_date.isnot(None))
            & (VehiclePositionEvents.start_time.isnot(None))
            & (VehiclePositionEvents.vehicle_id.isnot(None))
            & (dupe_hash_cte.c.hash.is_(None))
        )
        .cte(name="stop_vp")
    )

    stop_tu_cte = (
        sa.select(
            TripUpdateEvents.pk_id.label("fk_tu_stopped_event"),
            TripUpdateEvents.stop_sequence,
            sa.case(
                (
                    StaticStops.parent_station.is_(None),
                    TripUpdateEvents.stop_id,
                ),
                else_=StaticStops.parent_station,
            ).label("parent_station"),
            TripUpdateEvents.direction_id,
            TripUpdateEvents.route_id,
            TripUpdateEvents.start_date,
            TripUpdateEvents.start_time,
            TripUpdateEvents.vehicle_id,
        )
        .join(
            StaticStops,
            sa.and_(
                TripUpdateEvents.fk_static_timestamp == StaticStops.timestamp,
                TripUpdateEvents.stop_id == StaticStops.stop_id,
            ),
            isouter=True,
        )
        .where(
            (TripUpdateEvents.updated_on > last_full_event_update_query)
            & (TripUpdateEvents.stop_sequence.isnot(None))
            & (TripUpdateEvents.stop_id.isnot(None))
            & (TripUpdateEvents.direction_id.isnot(None))
            & (TripUpdateEvents.route_id.isnot(None))
            & (TripUpdateEvents.start_date.isnot(None))
            & (TripUpdateEvents.start_time.isnot(None))
            & (TripUpdateEvents.vehicle_id.isnot(None))
        )
        .cte(name="stop_tu")
    )

    first_join_cte = (
        sa.select(
            move_vp_cte.c.fk_vp_moving_event.label("fk_vp_moving_event"),
            stop_tu_cte.c.fk_tu_stopped_event.label("fk_tu_stopped_event"),
            sa.func.coalesce(
                move_vp_cte.c.stop_sequence,
                stop_tu_cte.c.stop_sequence,
            ).label("stop_sequence"),
            sa.func.coalesce(
                move_vp_cte.c.parent_station,
                stop_tu_cte.c.parent_station,
            ).label("parent_station"),
            sa.func.coalesce(
                move_vp_cte.c.direction_id,
                stop_tu_cte.c.direction_id,
            ).label("direction_id"),
            sa.func.coalesce(
                move_vp_cte.c.route_id,
                stop_tu_cte.c.route_id,
            ).label("route_id"),
            sa.func.coalesce(
                move_vp_cte.c.start_date,
                stop_tu_cte.c.start_date,
            ).label("start_date"),
            sa.func.coalesce(
                move_vp_cte.c.start_time,
                stop_tu_cte.c.start_time,
            ).label("start_time"),
            sa.func.coalesce(
                move_vp_cte.c.vehicle_id,
                stop_tu_cte.c.vehicle_id,
            ).label("vehicle_id"),
        )
        .join(
            stop_tu_cte,
            sa.and_(
                move_vp_cte.c.stop_sequence == stop_tu_cte.c.stop_sequence,
                move_vp_cte.c.parent_station == stop_tu_cte.c.parent_station,
                move_vp_cte.c.direction_id == stop_tu_cte.c.direction_id,
                move_vp_cte.c.route_id == stop_tu_cte.c.route_id,
                move_vp_cte.c.start_date == stop_tu_cte.c.start_date,
                move_vp_cte.c.start_time == stop_tu_cte.c.start_time,
                move_vp_cte.c.vehicle_id == stop_tu_cte.c.vehicle_id,
            ),
            full=True,
        )
    ).cte(name="first_join")

    merged_query = (
        sa.select(
            first_join_cte.c.fk_vp_moving_event,
            first_join_cte.c.fk_tu_stopped_event,
            stop_vp_cte.c.fk_vp_stopped_event,
            sa.func.coalesce(
                first_join_cte.c.stop_sequence,
                stop_vp_cte.c.stop_sequence,
            ).label("stop_sequence"),
            sa.func.coalesce(
                first_join_cte.c.parent_station,
                stop_vp_cte.c.parent_station,
            ).label("parent_station"),
            sa.func.coalesce(
                first_join_cte.c.direction_id,
                stop_vp_cte.c.direction_id,
            ).label("direction_id"),
            sa.func.coalesce(
                first_join_cte.c.route_id,
                stop_vp_cte.c.route_id,
            ).label("route_id"),
            sa.func.coalesce(
                first_join_cte.c.start_date,
                stop_vp_cte.c.start_date,
            ).label("start_date"),
            sa.func.coalesce(
                first_join_cte.c.start_time,
                stop_vp_cte.c.start_time,
            ).label("start_time"),
            sa.func.coalesce(
                first_join_cte.c.vehicle_id,
                stop_vp_cte.c.vehicle_id,
            ).label("vehicle_id"),
        )
        .join(
            stop_vp_cte,
            sa.and_(
                first_join_cte.c.stop_sequence == stop_vp_cte.c.stop_sequence,
                first_join_cte.c.parent_station == stop_vp_cte.c.parent_station,
                first_join_cte.c.direction_id == stop_vp_cte.c.direction_id,
                first_join_cte.c.route_id == stop_vp_cte.c.route_id,
                first_join_cte.c.start_date == stop_vp_cte.c.start_date,
                first_join_cte.c.start_time == stop_vp_cte.c.start_time,
                first_join_cte.c.vehicle_id == stop_vp_cte.c.vehicle_id,
            ),
            full=True,
        )
    )
        
    process_logger = ProcessLogger("l1_pull_events")
    process_logger.log_start()

    # get dataframe from L0 table select
    merged_dataframe = db_manager.select_as_dataframe(merged_query)

    # if L0 select produces no results, return empty frame
    if merged_dataframe.shape[0] == 0:
        return merged_dataframe

    # if data is retrieved from L0 table, ensure number columns are
    # Int64 type to ensure consistent hashing
    for column in convert_to_number_columns:
        merged_dataframe[column] = pandas.to_numeric(
            merged_dataframe[column]
        ).astype("Int64")

    # create hash column to be used as FullTripEvent primary key
    # drop category columns after hash has been created
    merged_dataframe = add_event_hash_column(
        merged_dataframe,
        expected_hash_columns=expected_hash_columns,
    )[merged_columns_to_keep]

    process_logger.log_complete()

    return merged_dataframe


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
    process_logger = ProcessLogger("l1_load_table")
    process_logger.log_start()

    # get dataframe of hashes represnting records in FullTripEvent table that
    # need to be update from loading TempFullTripEvents table
    hash_to_update_query = sa.select(TempFullTripEvents.hash).join(
        FullTripEvents,
        FullTripEvents.hash == TempFullTripEvents.hash,
    )
    hashes_to_update = db_manager.select_as_dataframe(hash_to_update_query)

    process_logger.add_metadata(total_updated=hashes_to_update.shape[0])

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
    process_logger.add_metadata(fk_vp_moving_events_updated=0)
    if vp_moving_mask.sum() > 0:
        result = db_manager.execute_with_data(
            update_query,
            insert_dataframe.loc[
                vp_moving_mask, ["hash", "fk_vp_moving_event"]
            ].rename(columns={"hash": "b_hash"}),
        )
        process_logger.add_metadata(fk_vp_moving_events_updated=result.rowcount)

    # mask for insert dataframe and vp_stopped events
    vp_stopped_mask = (~insert_dataframe["fk_vp_stopped_event"].isna()) & (
        to_update_mask
    )
    # if mask shows records need updating, execute update
    process_logger.add_metadata(fk_vp_stopped_events_updated=0)
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

    # mask for insert dataframe and tu_stopped events
    tu_stopped_mask = (~insert_dataframe["fk_tu_stopped_event"].isna()) & (
        to_update_mask
    )
    # if mask shows records need updating, execute update
    process_logger.add_metadata(fk_tu_stopped_events_updated=0)
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

    process_logger = ProcessLogger("l1_load_table")
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
            )
            .join(
                FullTripEvents,
                FullTripEvents.hash == TempFullTripEvents.hash,
                isouter=True,
            )
            .where(
                (FullTripEvents.hash.is_(None))
                & sa.or_(
                    TempFullTripEvents.fk_vp_moving_event.isnot(None),
                    TempFullTripEvents.fk_vp_stopped_event.isnot(None),
                )
            ),
        )
    )
    result = db_manager.execute(insert_query)

    process_logger.add_metadata(total_inserted=result.rowcount)
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
        insert_dataframe = pull_and_transform(db_manager)
        process_logger.add_metadata(
            temp_table_record_count=insert_dataframe.shape[0]
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
