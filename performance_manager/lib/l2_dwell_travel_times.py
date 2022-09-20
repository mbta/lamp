import sqlalchemy as sa

from .logging_utils import ProcessLogger
from .postgres_utils import DatabaseManager
from .postgres_schema import (
    VehiclePositionEvents,
    FullTripEvents,
    DwellTimes,
    TravelTimes,
)


def dwell_times(db_manager: DatabaseManager) -> None:
    """
    perform update on dwell times table

    executed as delete and insert
    """
    process_logger = ProcessLogger("update l2 dwell times")
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
            DwellTimes.pk_id.in_(
                sa.select(DwellTimes.pk_id).join(
                    insert_cte, insert_cte.c.pk_id == DwellTimes.pk_id
                )
            )
        )
        .execution_options(synchronize_session="fetch")
    )
    delete_result = db_manager.execute(delete_query)
    process_logger.add_metadata(rows_updated=delete_result.rowcount)

    # insert new dwell time records
    insert_query = DwellTimes.__table__.insert().from_select(
        ["pk_id", "dwell_time_seconds"], insert_select
    )
    insert_result = db_manager.execute(insert_query)
    process_logger.add_metadata(rows_inserted=insert_result.rowcount)

    process_logger.log_complete()


def travel_times(db_manager: DatabaseManager) -> None:
    """
    perform update on travel times table

    executed as delete and insert
    """
    process_logger = ProcessLogger("update l2 travel times")
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
            TravelTimes.pk_id.in_(
                sa.select(TravelTimes.pk_id).join(
                    insert_cte, insert_cte.c.pk_id == TravelTimes.pk_id
                )
            )
        )
        .execution_options(synchronize_session="fetch")
    )
    delete_result = db_manager.execute(delete_query)
    process_logger.add_metadata(rows_updated=delete_result.rowcount)

    # insert new travel time records
    insert_query = TravelTimes.__table__.insert().from_select(
        ["pk_id", "travel_time_seconds"], insert_select
    )
    insert_result = db_manager.execute(insert_query)
    process_logger.add_metadata(rows_inserted=insert_result.rowcount)

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
    process_logger = ProcessLogger("process_l2_dwell_and_travel")
    process_logger.log_start()
    try:

        dwell_times(db_manager=db_manager)
        travel_times(db_manager=db_manager)

        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)
