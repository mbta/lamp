from typing import Optional
import sqlalchemy as sa

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    FullTripEvents,
    TripUpdateEvents,
    StaticStops,
    Headways,
    TempHeadways,
)


def get_min_timestamp(db_manager: DatabaseManager) -> Optional[int]:
    """
    find the minimum timestamp_start value for processing new headways
    """
    # query for trip update timestamp_start values
    tu_timestamp_query = sa.select(
        sa.func.min(TripUpdateEvents.timestamp_start).label("timestamp_start"),
    ).join(
        FullTripEvents,
        FullTripEvents.fk_tu_stopped_event == TripUpdateEvents.pk_id,
    )

    # query for vehicle position timestamp_start values
    vp_timestamp_query = sa.select(
        sa.func.min(VehiclePositionEvents.timestamp_start).label(
            "timestamp_start"
        ),
    ).join(
        FullTripEvents,
        FullTripEvents.fk_vp_stopped_event == VehiclePositionEvents.pk_id,
    )

    # get maximum updated_on timestamp from headways table
    max_headway_update_query = sa.select(sa.func.max(Headways.updated_on))
    # mypy error: error: "BaseCursorResult" has no attribute "fetchone"
    max_update_dt = db_manager.execute(max_headway_update_query).fetchone()[0]  # type: ignore

    # will only be None if no records exist in Headways table
    # if records do exist, add constraing to vehicle position and trip update
    # timestamp_start queries
    if max_update_dt is not None:
        tu_timestamp_query = tu_timestamp_query.where(
            FullTripEvents.updated_on > max_update_dt
        )
        vp_timestamp_query = vp_timestamp_query.where(
            FullTripEvents.updated_on > max_update_dt
        )

    # union query so all results are combined
    min_timestamp_start_cte = tu_timestamp_query.union(vp_timestamp_query).cte(
        name="min_timestamp"
    )

    # get minimum timestamp_start value for records needing to be added to
    # headways table
    min_timestamp_start_query = sa.select(
        sa.func.min(min_timestamp_start_cte.c.timestamp_start)
    )

    # this will return a valid timestamp_start integer or None, if no records
    # need to be added to headways table
    # mypy error: error: "BaseCursorResult" has no attribute "fetchone"
    min_timestamp_start = db_manager.execute(min_timestamp_start_query).fetchone()[0]  # type: ignore

    # pull overlapping timestamp start values going back 6 Hours
    # this effecitvely sets a maxium headway value of 6 hours
    # it is assumed that headways over 6 hours are not really headways and do not
    # need to be recorded
    if isinstance(min_timestamp_start, int):
        min_timestamp_start -= 60 * 60 * 6

    # return None or int value
    return min_timestamp_start


def load_temp_headways(db_manager: DatabaseManager) -> bool:
    """
    Load headways data from new records into Temporary holding table

    return True if any new records are loaded

    return False if NO new records loaded
    """
    process_logger = ProcessLogger("l2_load_temp_headways")
    process_logger.log_start()

    # get minimum timestamp_start value for records that need to be processed
    # for headways table
    # will return None if no headways need to be processed
    min_timestamp_start = get_min_timestamp(db_manager)
    if min_timestamp_start is None:
        process_logger.add_metadata(temp_rows_inserted=0)
        process_logger.log_complete()
        return False

    # headways will be aggregated by the following fields:
    # - direction_id
    # - route_id
    # - start_date
    # - parent_station
    #
    # query for stop event records from vehcile positions that require headways
    # processing
    vp_query = (
        sa.select(
            FullTripEvents.fk_vp_stopped_event.label("fk_vp_stopped_event"),
            sa.literal(None).label("fk_tu_stopped_event"),
            FullTripEvents.hash,
            VehiclePositionEvents.direction_id,
            VehiclePositionEvents.route_id,
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.vehicle_id,
            sa.case(
                (
                    StaticStops.parent_station.isnot(None),
                    StaticStops.parent_station,
                ),
                else_=VehiclePositionEvents.stop_id,
            ).label("parent_station"),
            VehiclePositionEvents.timestamp_start,
        )
        .join(
            VehiclePositionEvents,
            FullTripEvents.fk_vp_stopped_event == VehiclePositionEvents.pk_id,
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
            sa.and_(
                FullTripEvents.fk_vp_stopped_event.isnot(None),
                VehiclePositionEvents.timestamp_start > min_timestamp_start,
            )
        )
    )

    # query for stop event records from trip updates that require headways
    # processing
    tu_query = (
        sa.select(
            sa.literal(None).label("fk_vp_stopped_event"),
            FullTripEvents.fk_tu_stopped_event.label("fk_tu_stopped_event"),
            FullTripEvents.hash,
            TripUpdateEvents.direction_id,
            TripUpdateEvents.route_id,
            TripUpdateEvents.start_date,
            TripUpdateEvents.vehicle_id,
            sa.case(
                (
                    StaticStops.parent_station.isnot(None),
                    StaticStops.parent_station,
                ),
                else_=TripUpdateEvents.stop_id,
            ).label("parent_station"),
            TripUpdateEvents.timestamp_start,
        )
        .join(
            TripUpdateEvents,
            FullTripEvents.fk_tu_stopped_event == TripUpdateEvents.pk_id,
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
            sa.and_(
                FullTripEvents.fk_tu_stopped_event.isnot(None),
                FullTripEvents.fk_vp_stopped_event.is_(None),
                TripUpdateEvents.timestamp_start > min_timestamp_start,
            )
        )
    )

    # merge stop event results from vehcile positions and trip updates into
    # one query all records should be unique
    stop_events_cte = vp_query.union_all(tu_query).cte(name="all_stop_events")

    # this subquery performs lag operation on vehicle_id and timestamp_start
    # fields, these lag operations create the fields of prev_vehicle_id and
    # prev_timestamp_start for every record
    # (except first record of partition groups)
    filter_query = sa.select(
        stop_events_cte.c.fk_vp_stopped_event,
        stop_events_cte.c.fk_tu_stopped_event,
        stop_events_cte.c.hash,
        stop_events_cte.c.direction_id,
        stop_events_cte.c.route_id,
        stop_events_cte.c.start_date,
        stop_events_cte.c.parent_station,
        stop_events_cte.c.vehicle_id,
        sa.func.lag(stop_events_cte.c.vehicle_id)
        .over(
            order_by=stop_events_cte.c.timestamp_start,
            partition_by=(
                stop_events_cte.c.direction_id,
                stop_events_cte.c.route_id,
                stop_events_cte.c.start_date,
                stop_events_cte.c.parent_station,
            ),
        )
        .label("prev_vehicle_id"),
        stop_events_cte.c.timestamp_start,
        sa.func.lag(stop_events_cte.c.timestamp_start)
        .over(
            order_by=stop_events_cte.c.timestamp_start,
            partition_by=(
                stop_events_cte.c.direction_id,
                stop_events_cte.c.route_id,
                stop_events_cte.c.start_date,
                stop_events_cte.c.parent_station,
            ),
        )
        .label("prev_timestamp_start"),
    ).subquery(name="filter_query")

    # this subquery is responsible for filtering duplicative stop events
    # it will filter out records where the same vehicle is recorded at the same
    # partition group within 3 minutes
    # (likely impossible but requires verification)
    headways_lag_query = (
        sa.select(
            filter_query.c.fk_vp_stopped_event,
            filter_query.c.fk_tu_stopped_event,
            filter_query.c.hash,
            filter_query.c.timestamp_start,
            sa.func.lag(filter_query.c.timestamp_start)
            .over(
                order_by=filter_query.c.timestamp_start,
                partition_by=(
                    filter_query.c.direction_id,
                    filter_query.c.route_id,
                    filter_query.c.start_date,
                    filter_query.c.parent_station,
                ),
            )
            .label("prev_timestamp_start"),
        )
        .where(
            sa.or_(
                filter_query.c.vehicle_id != filter_query.c.prev_vehicle_id,
                filter_query.c.timestamp_start
                - filter_query.c.prev_timestamp_start
                > 180,
            )
        )
        .subquery(name="headways_lag")
    )

    # final select statement to be used for insertion of records into temporary
    # headways table
    headways_final_query = sa.select(
        headways_lag_query.c.fk_vp_stopped_event,
        headways_lag_query.c.fk_tu_stopped_event,
        headways_lag_query.c.hash.label("fk_trip_event_hash"),
        (
            headways_lag_query.c.timestamp_start
            - headways_lag_query.c.prev_timestamp_start
        ).label("headway_seconds"),
    )

    insert_cols = [
        "fk_vp_stopped_event",
        "fk_tu_stopped_event",
        "fk_trip_event_hash",
        "headway_seconds",
    ]

    insert_query = (
        TempHeadways.metadata.tables[TempHeadways.__tablename__]
        .insert()
        .from_select(
            insert_cols,
            headways_final_query,
        )
    )

    db_manager.truncate_table(TempHeadways)
    result = db_manager.execute(insert_query)

    process_logger.add_metadata(temp_rows_inserted=result.rowcount)
    process_logger.log_complete()

    if result.rowcount > 0:
        return True

    return False


def update_headways_from_temp(db_manager: DatabaseManager) -> None:
    """
    update headways tables with UPDATE from SELECT of TempHeadways table
    """
    process_logger = ProcessLogger("l2_headways_update")
    process_logger.log_start()
    update_query = (
        Headways.metadata.tables[Headways.__tablename__]
        .update()
        .values(
            fk_trip_event_hash=TempHeadways.fk_trip_event_hash,
            fk_tu_stopped_event=TempHeadways.fk_tu_stopped_event,
            fk_vp_stopped_event=TempHeadways.fk_vp_stopped_event,
            headway_seconds=TempHeadways.headway_seconds,
        )
        .where(Headways.fk_trip_event_hash == TempHeadways.fk_trip_event_hash)
    )

    result = db_manager.execute(update_query)

    process_logger.add_metadata(rows_updated=result.rowcount)
    process_logger.log_complete()


def insert_new_headways(db_manager: DatabaseManager) -> None:
    """
    insert new headways into Headways table with INSERT from SELECT of
    TempHeadways table
    """
    insert_cols = [
        "fk_vp_stopped_event",
        "fk_tu_stopped_event",
        "fk_trip_event_hash",
        "headway_seconds",
    ]

    process_logger = ProcessLogger("l2_headways_insert")
    process_logger.log_start()
    insert_query = (
        Headways.metadata.tables[Headways.__tablename__]
        .insert()
        .from_select(
            insert_cols,
            sa.select(
                TempHeadways.fk_vp_stopped_event,
                TempHeadways.fk_tu_stopped_event,
                TempHeadways.fk_trip_event_hash,
                TempHeadways.headway_seconds,
            )
            .join(
                Headways,
                Headways.fk_trip_event_hash == TempHeadways.fk_trip_event_hash,
                isouter=True,
            )
            .where((Headways.fk_trip_event_hash.is_(None))),
        )
    )
    result = db_manager.execute(insert_query)

    process_logger.add_metadata(rows_inserted=result.rowcount)
    process_logger.log_complete()


def process_headways(db_manager: DatabaseManager) -> None:
    """
    process new events from L1 FullTripEvents table and insert into
    Headways table

    """
    process_logger = ProcessLogger("process_l2_headways")
    process_logger.log_start()
    try:
        # gate to check if no records need update / insert
        if load_temp_headways(db_manager) is False:
            process_logger.log_complete()
            return

        update_headways_from_temp(db_manager)
        insert_new_headways(db_manager)

        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)
