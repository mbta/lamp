import datetime
import calendar
import sqlalchemy as sa

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    FullTripEvents,
    DwellTimes,
    TravelTimes,
    StaticCalendar,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
)


def dwell_times(db_manager: DatabaseManager) -> None:
    """
    perform update on dwell times table

    executed as delete and insert
    """
    process_logger = ProcessLogger("l2_load_table", table_type="dwell_times")
    process_logger.log_start()

    # base select for insert of records dwell times
    # dwell time = timestamp.end - timestamp.start for stop portion of trip events
    insert_select = sa.select(
        VehiclePositionEvents.pk_id,
        (
            VehiclePositionEvents.timestamp_end
            - VehiclePositionEvents.timestamp_start
        ).label("dwell_time_seconds"),
    ).join(
        FullTripEvents,
        VehiclePositionEvents.pk_id == FullTripEvents.fk_vp_stopped_event,
    )

    # pull last created_on timestamp from dwell time table
    max_create_travel_query = sa.select(sa.func.max(DwellTimes.created_on))
    # mypy error: error: "BaseCursorResult" has no attribute "fetchone"
    max_create_dt = db_manager.execute(max_create_travel_query).fetchone()[0]  # type: ignore

    # if last created_on timestamp exists, add to to query
    # limit insert records to events with updated_on timestamp after last
    # created_on timestamp from dwell time table
    if max_create_dt is not None:
        insert_select = insert_select.where(
            VehiclePositionEvents.updated_on > max_create_dt
        )

    # create CTE for delete query
    insert_cte = insert_select.cte("tt_inserts")

    # delete records from dwell times that overlap with new insert records
    delete_query = (
        DwellTimes.__table__.delete()
        .where(
            DwellTimes.fk_dwell_time_id.in_(
                sa.select(DwellTimes.fk_dwell_time_id).join(
                    insert_cte,
                    insert_cte.c.pk_id == DwellTimes.fk_dwell_time_id,
                )
            )
        )
        .execution_options(synchronize_session="fetch")
    )
    delete_result = db_manager.execute(delete_query)
    process_logger.add_metadata(total_updated=delete_result.rowcount)

    # insert new dwell time records
    insert_query = DwellTimes.__table__.insert().from_select(
        ["fk_dwell_time_id", "dwell_time_seconds"], insert_select
    )
    insert_result = db_manager.execute(insert_query)
    process_logger.add_metadata(total_inserted=insert_result.rowcount)

    process_logger.log_complete()


def travel_times(db_manager: DatabaseManager) -> None:
    """
    perform update on travel times table

    executed as delete and insert
    """
    process_logger = ProcessLogger("l2_load_table", table_type="travel_times")
    process_logger.log_start()

    # base select for insert of records travel times
    # travel time = timestamp.end - timestamp.start for moving portion of trip events
    insert_select = sa.select(
        VehiclePositionEvents.pk_id,
        (
            VehiclePositionEvents.timestamp_end
            - VehiclePositionEvents.timestamp_start
        ).label("travel_time_seconds"),
    ).join(
        FullTripEvents,
        VehiclePositionEvents.pk_id == FullTripEvents.fk_vp_moving_event,
    )

    # pull last created_on timestamp from travel time table
    max_create_travel_query = sa.select(sa.func.max(TravelTimes.created_on))
    # mypy error: error: "BaseCursorResult" has no attribute "fetchone"
    max_create_dt = db_manager.execute(max_create_travel_query).fetchone()[0]  # type: ignore

    # if last created_on timestamp exists, add to to query
    # limit insert records to events with updated_on timestamp after last
    # created_on timestamp from travel time table
    if max_create_dt is not None:
        insert_select = insert_select.where(
            VehiclePositionEvents.updated_on > max_create_dt
        )

    # create CTE for delete query
    insert_cte = insert_select.cte("tt_inserts")

    # delete records from travel times that overlap with new insert records
    delete_query = (
        TravelTimes.__table__.delete()
        .where(
            TravelTimes.fk_travel_time_id.in_(
                sa.select(TravelTimes.fk_travel_time_id).join(
                    insert_cte,
                    insert_cte.c.pk_id == TravelTimes.fk_travel_time_id,
                )
            )
        )
        .execution_options(synchronize_session="fetch")
    )
    delete_result = db_manager.execute(delete_query)
    process_logger.add_metadata(total_updated=delete_result.rowcount)

    # insert new travel time records
    insert_query = TravelTimes.__table__.insert().from_select(
        ["fk_travel_time_id", "travel_time_seconds"], insert_select
    )
    insert_result = db_manager.execute(insert_query)
    process_logger.add_metadata(total_inserted=insert_result.rowcount)

    process_logger.log_complete()


def expected_travel_times(db_manager: DatabaseManager) -> None:
    """
    update travel times table to add expected travel times from GTFS
    static schedule data

    executed as update from
    """
    process_logger = ProcessLogger(
        "l2_load_table", table_type="expected_travel_times"
    )
    process_logger.log_start()

    rows_updated = 0

    seed_vars_query = (
        sa.select(
            (
                VehiclePositionEvents.start_date,
                VehiclePositionEvents.fk_static_timestamp,
            ),
            distinct=(
                VehiclePositionEvents.start_date,
                VehiclePositionEvents.fk_static_timestamp,
            ),
        )
        .join(
            TravelTimes,
            TravelTimes.fk_travel_time_id == VehiclePositionEvents.pk_id,
        )
        .where(
            (TravelTimes.expected_travel_time_seconds.is_(sa.null())),
        )
        .order_by(
            VehiclePositionEvents.start_date,
            VehiclePositionEvents.fk_static_timestamp,
        )
    )

    # update a maximum of 3 start_date and static timestamp combinations
    for seed in db_manager.select_as_list(seed_vars_query)[:3]:
        seed_start_date = seed["start_date"]
        seed_timestamp = seed["fk_static_timestamp"]

        date = datetime.datetime.strptime(str(seed_start_date), "%Y%m%d")
        day_of_week = calendar.day_name[date.weekday()].lower()

        # CTE to produce expected travels time from GTFS static calendar data
        expect_travel_time_cte = (
            sa.select(
                StaticTrips.route_id,
                sa.cast(StaticTrips.direction_id, sa.Integer).label(
                    "direction_id"
                ),
                sa.func.coalesce(
                    StaticStops.parent_station,
                    StaticStops.stop_id,
                ).label("parent_station"),
                sa.func.min(StaticStopTimes.arrival_time)
                .over(
                    partition_by=StaticStopTimes.trip_id,
                )
                .label(name="start_time"),
                (
                    StaticStopTimes.arrival_time
                    - sa.func.lag(StaticStopTimes.departure_time).over(
                        partition_by=StaticStopTimes.trip_id,
                        order_by=StaticStopTimes.arrival_time,
                    )
                ).label("expected_travel_time_seconds"),
            )
            .select_from(StaticStopTimes)
            .join(
                StaticTrips,
                sa.and_(
                    StaticStopTimes.timestamp == StaticTrips.timestamp,
                    StaticStopTimes.trip_id == StaticTrips.trip_id,
                ),
            )
            .join(
                StaticStops,
                sa.and_(
                    StaticStopTimes.timestamp == StaticStops.timestamp,
                    StaticStopTimes.stop_id == StaticStops.stop_id,
                ),
            )
            .join(
                StaticCalendar,
                sa.and_(
                    StaticStopTimes.timestamp == StaticCalendar.timestamp,
                    StaticTrips.service_id == StaticCalendar.service_id,
                ),
            )
            .where(
                (StaticStopTimes.timestamp == seed_timestamp)
                & (StaticTrips.timestamp == seed_timestamp)
                & (StaticStops.timestamp == seed_timestamp)
                & (StaticCalendar.timestamp == seed_timestamp)
                & (getattr(StaticCalendar, day_of_week) == sa.true())
                & (StaticCalendar.start_date <= int(seed_start_date))
                & (StaticCalendar.end_date >= int(seed_start_date))
            )
            .cte(name="expect_travel_time")
        )

        # CTE to gather actual travel time records needing updating
        # CAST direction_id as integer because direction_id stored as different
        # var types between GTFS-RT and GTFS Static data (smallint vs bool)
        actual_travel_time_cte = (
            sa.select(
                TravelTimes.fk_travel_time_id,
                VehiclePositionEvents.route_id,
                sa.cast(VehiclePositionEvents.direction_id, sa.Integer).label(
                    "direction_id"
                ),
                sa.func.coalesce(
                    StaticStops.parent_station,
                    StaticStops.stop_id,
                ).label("parent_station"),
                VehiclePositionEvents.start_time,
            )
            .select_from(VehiclePositionEvents)
            .join(
                StaticStops,
                sa.and_(
                    StaticStops.stop_id == VehiclePositionEvents.stop_id,
                    StaticStops.timestamp
                    == VehiclePositionEvents.fk_static_timestamp,
                ),
            )
            .join(
                TravelTimes,
                TravelTimes.fk_travel_time_id == VehiclePositionEvents.pk_id,
            )
            .where(
                (VehiclePositionEvents.fk_static_timestamp == seed_timestamp)
                & (VehiclePositionEvents.start_date == int(seed_start_date))
                & (StaticStops.timestamp == seed_timestamp)
                & (TravelTimes.expected_travel_time_seconds.is_(sa.null()))
            )
            .limit(250_000)
            .cte(name="actual_travel_time")
        )

        # select to generate expected_travel_time_seconds from gtfs static data
        # this query does not JOIN on sequence_id because sequence_id's are
        # not guaranteed to matched between the actual and expected data.
        #
        # where no matching expected_travel_time exists in the gtfs data,
        # value will be filled with -1
        update_select = (
            sa.select(
                (
                    actual_travel_time_cte.c.fk_travel_time_id,
                    sa.func.coalesce(
                        expect_travel_time_cte.c.expected_travel_time_seconds,
                        -1,
                    ).label("expected_travel_time_seconds"),
                ),
                distinct=actual_travel_time_cte.c.fk_travel_time_id,
            )
            .select_from(actual_travel_time_cte)
            .join(
                expect_travel_time_cte,
                sa.and_(
                    actual_travel_time_cte.c.route_id
                    == expect_travel_time_cte.c.route_id,
                    actual_travel_time_cte.c.direction_id
                    == expect_travel_time_cte.c.direction_id,
                    actual_travel_time_cte.c.parent_station
                    == expect_travel_time_cte.c.parent_station,
                    actual_travel_time_cte.c.start_time
                    <= expect_travel_time_cte.c.start_time,
                ),
                isouter=True,
            )
            .order_by(
                actual_travel_time_cte.c.fk_travel_time_id,
                expect_travel_time_cte.c.start_time,
            )
            .subquery(name="expect_travel_time_updates")
        )

        # update query will update existing TravelTimes table from update_select
        # statemet
        update_query = (
            sa.update(TravelTimes)
            .where(
                TravelTimes.fk_travel_time_id
                == update_select.c.fk_travel_time_id
            )
            .values(
                expected_travel_time_seconds=update_select.c.expected_travel_time_seconds
            )
        )

        update_result = db_manager.execute(update_query)
        rows_updated += update_result.rowcount

    process_logger.add_metadata(total_updated=rows_updated)

    process_logger.log_complete()


def process_dwell_travel_times(db_manager: DatabaseManager) -> None:
    """
    responsible for inserting new records into dwell and travel times tables

    these are grouped because the basic logic behind these tables is very similar

    for moving events, travel times are calculated by:
        timestamp_end - timestamp_start from VehiclePositionEvents

    for stopped events, dwell times are calculated by:
        timestamp_end - timestamp_start from VehiclePositionEvents

    dwell and travel times tables are managed as INSERT-ONLY tables,
    records requiring updating are first deleted and then all new/updated
    records are inserted
    """
    process_logger = ProcessLogger("l2_load_tables")
    process_logger.log_start()
    try:

        dwell_times(db_manager=db_manager)
        travel_times(db_manager=db_manager)
        expected_travel_times(db_manager=db_manager)

        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)
