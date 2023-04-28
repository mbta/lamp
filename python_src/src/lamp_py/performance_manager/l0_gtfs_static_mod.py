import sqlalchemy as sa


from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    StaticStopTimes,
    StaticStops,
    StaticTrips,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger


def generate_scheduled_travel_times(
    fk_timestamp: int, db_manager: DatabaseManager
) -> None:
    """
    generate scheduled travel_times and insert into static_stop_times table
    """
    travel_times_cte = (
        sa.select(
            StaticStopTimes.pk_id,
            (
                StaticStopTimes.arrival_time
                - sa.func.lag(StaticStopTimes.departure_time).over(
                    partition_by=StaticStopTimes.trip_id,
                    order_by=StaticStopTimes.stop_sequence,
                )
            ).label("schedule_travel_time_seconds"),
        )
        .select_from(StaticStopTimes)
        .where(
            StaticStopTimes.timestamp == fk_timestamp,
        )
        .cte("scheduled_travel_times")
    )

    update_q = (
        sa.update(StaticStopTimes.__table__)
        .where(
            StaticStopTimes.pk_id == travel_times_cte.c.pk_id,
        )
        .values(
            schedule_travel_time_seconds=travel_times_cte.c.schedule_travel_time_seconds,
        )
    )

    process_logger = ProcessLogger("gtfs.generate_scheduled_travel_times")
    process_logger.log_start()
    db_manager.execute(update_q)
    process_logger.log_complete()


def generate_scheduled_branch_headways(
    fk_timestamp: int, db_manager: DatabaseManager
) -> None:
    """
    generate scheduled branch headways and insert into static_stop_times table
    """
    branch_headways_cte = (
        sa.select(
            StaticStopTimes.pk_id,
            (
                StaticStopTimes.departure_time
                - sa.func.lag(StaticStopTimes.departure_time).over(
                    partition_by=[
                        StaticStops.parent_station,
                        StaticTrips.service_id,
                        StaticTrips.direction_id,
                        StaticTrips.branch_route_id,
                    ],
                    order_by=StaticStopTimes.departure_time,
                )
            ).label("branch_headways"),
        )
        .select_from(StaticStopTimes)
        .join(
            StaticTrips,
            sa.and_(
                StaticStopTimes.trip_id == StaticTrips.trip_id,
                StaticStopTimes.timestamp == StaticTrips.timestamp,
            ),
        )
        .join(
            StaticStops,
            sa.and_(
                StaticStopTimes.stop_id == StaticStops.stop_id,
                StaticStopTimes.timestamp == StaticStops.timestamp,
            ),
        )
        .where(
            StaticStopTimes.timestamp == fk_timestamp,
            StaticTrips.branch_route_id.is_not(None),
        )
        .cte("scheduled_branch_headways")
    )

    update_q = (
        sa.update(StaticStopTimes.__table__)
        .where(
            StaticStopTimes.pk_id == branch_headways_cte.c.pk_id,
        )
        .values(
            schedule_headway_branch_seconds=branch_headways_cte.c.branch_headways,
        )
    )

    process_logger = ProcessLogger("gtfs.generate_scheduled_branch_headways")
    process_logger.log_start()
    db_manager.execute(update_q)
    process_logger.log_complete()


def generate_scheduled_trunk_headways(
    fk_timestamp: int, db_manager: DatabaseManager
) -> None:
    """
    generate scheduled trunk headways and insert into static_stop_times table
    """
    trunk_headways_cte = (
        sa.select(
            StaticStopTimes.pk_id,
            (
                StaticStopTimes.departure_time
                - sa.func.lag(StaticStopTimes.departure_time).over(
                    partition_by=[
                        StaticStops.parent_station,
                        StaticTrips.service_id,
                        StaticTrips.direction_id,
                        StaticTrips.trunk_route_id,
                    ],
                    order_by=StaticStopTimes.departure_time,
                )
            ).label("trunk_headways"),
        )
        .select_from(StaticStopTimes)
        .join(
            StaticTrips,
            sa.and_(
                StaticStopTimes.trip_id == StaticTrips.trip_id,
                StaticStopTimes.timestamp == StaticTrips.timestamp,
            ),
        )
        .join(
            StaticStops,
            sa.and_(
                StaticStopTimes.stop_id == StaticStops.stop_id,
                StaticStopTimes.timestamp == StaticStops.timestamp,
            ),
        )
        .where(
            StaticStopTimes.timestamp == fk_timestamp,
            StaticTrips.trunk_route_id.is_not(None),
        )
        .cte("scheduled_trunk_headways")
    )

    update_q = (
        sa.update(StaticStopTimes.__table__)
        .where(
            StaticStopTimes.pk_id == trunk_headways_cte.c.pk_id,
        )
        .values(
            schedule_headway_trunk_seconds=trunk_headways_cte.c.trunk_headways,
        )
    )

    process_logger = ProcessLogger("gtfs.generate_scheduled_trunk_headways")
    process_logger.log_start()
    db_manager.execute(update_q)
    process_logger.log_complete()


def modify_static_tables(
    fk_timestamp: int, db_manager: DatabaseManager
) -> None:
    """
    This function is responsible for modifying any GTFS static schedule tables after
    a new schedule as been loaded

    currently, we are only addding pre-computed metrics values to the static_stop_times tables
    """
    generate_scheduled_travel_times(fk_timestamp, db_manager)
    generate_scheduled_branch_headways(fk_timestamp, db_manager)
    generate_scheduled_trunk_headways(fk_timestamp, db_manager)
