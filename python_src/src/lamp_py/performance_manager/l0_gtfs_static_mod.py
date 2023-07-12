import sqlalchemy as sa


from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    StaticStopTimes,
    StaticStops,
    StaticTrips,
    TempStaticHeadwaysGen,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger


def generate_scheduled_travel_times(
    static_version_key: int, db_manager: DatabaseManager
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
            StaticStopTimes.static_version_key == static_version_key,
        )
        .subquery("scheduled_travel_times")
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


def generate_scheduled_branch_headways(db_manager: DatabaseManager) -> None:
    """
    generate scheduled branch headways and insert into static_stop_times table
    """
    branch_headways = (
        sa.select(
            TempStaticHeadwaysGen.pk_id,
            (
                TempStaticHeadwaysGen.departure_time
                - sa.func.lag(TempStaticHeadwaysGen.departure_time).over(
                    partition_by=[
                        TempStaticHeadwaysGen.parent_station,
                        TempStaticHeadwaysGen.service_id,
                        TempStaticHeadwaysGen.direction_id,
                        TempStaticHeadwaysGen.branch_route_id,
                    ],
                    order_by=TempStaticHeadwaysGen.departure_time,
                )
            ).label("branch_headways"),
        )
        .select_from(TempStaticHeadwaysGen)
        .where(
            TempStaticHeadwaysGen.branch_route_id.is_not(None),
        )
        .subquery("scheduled_branch_headways")
    )

    update_q = (
        sa.update(StaticStopTimes.__table__)
        .where(
            StaticStopTimes.pk_id == branch_headways.c.pk_id,
        )
        .values(
            schedule_headway_branch_seconds=branch_headways.c.branch_headways,
        )
    )

    process_logger = ProcessLogger("gtfs.generate_scheduled_branch_headways")
    process_logger.log_start()
    db_manager.execute(update_q)
    process_logger.log_complete()


def generate_scheduled_trunk_headways(db_manager: DatabaseManager) -> None:
    """
    generate scheduled trunk headways and insert into static_stop_times table
    """
    trunk_headways = (
        sa.select(
            TempStaticHeadwaysGen.pk_id,
            (
                TempStaticHeadwaysGen.departure_time
                - sa.func.lag(TempStaticHeadwaysGen.departure_time).over(
                    partition_by=[
                        TempStaticHeadwaysGen.parent_station,
                        TempStaticHeadwaysGen.service_id,
                        TempStaticHeadwaysGen.direction_id,
                        TempStaticHeadwaysGen.trunk_route_id,
                    ],
                    order_by=TempStaticHeadwaysGen.departure_time,
                )
            ).label("trunk_headways"),
        )
        .select_from(TempStaticHeadwaysGen)
        .where(
            TempStaticHeadwaysGen.trunk_route_id.is_not(None),
        )
        .subquery("scheduled_trunk_headways")
    )

    update_q = (
        sa.update(StaticStopTimes.__table__)
        .where(
            StaticStopTimes.pk_id == trunk_headways.c.pk_id,
        )
        .values(
            schedule_headway_trunk_seconds=trunk_headways.c.trunk_headways,
        )
    )

    process_logger = ProcessLogger("gtfs.generate_scheduled_trunk_headways")
    process_logger.log_start()
    db_manager.execute(update_q)
    process_logger.log_complete()


def load_temp_table_headway_gen(
    static_version_key: int, db_manager: DatabaseManager
) -> None:
    """
    load static schedule data into temp table for generating scheduled headways

    this was selected because executing an update as a CTE statement causes very poor performance
    """
    load_headways_gen = (
        sa.select(
            StaticStopTimes.pk_id,
            StaticStopTimes.departure_time,
            sa.func.coalesce(
                StaticStops.parent_station, StaticStops.stop_id
            ).label("parent_station"),
            StaticTrips.service_id,
            StaticTrips.direction_id,
            StaticTrips.trunk_route_id,
            StaticTrips.branch_route_id,
        )
        .select_from(StaticStopTimes)
        .join(
            StaticStops,
            sa.and_(
                StaticStops.stop_id == StaticStopTimes.stop_id,
                StaticStops.static_version_key
                == StaticStopTimes.static_version_key,
            ),
        )
        .join(
            StaticTrips,
            sa.and_(
                StaticTrips.trip_id == StaticStopTimes.trip_id,
                StaticTrips.static_version_key
                == StaticStopTimes.static_version_key,
            ),
        )
        .where(
            StaticStopTimes.static_version_key == static_version_key,
            StaticStops.static_version_key == static_version_key,
            StaticTrips.static_version_key == static_version_key,
        )
    )
    columns_to_insert = [
        "pk_id",
        "departure_time",
        "parent_station",
        "service_id",
        "direction_id",
        "trunk_route_id",
        "branch_route_id",
    ]
    insert_from_select = sa.insert(TempStaticHeadwaysGen.__table__).from_select(
        columns_to_insert, load_headways_gen
    )

    process_logger = ProcessLogger("gtfs.load_headways_gen_temp")
    process_logger.log_start()
    db_manager.truncate_table(TempStaticHeadwaysGen)
    db_manager.execute(insert_from_select)
    process_logger.log_complete()


def modify_static_tables(
    static_version_key: int, db_manager: DatabaseManager
) -> None:
    """
    This function is responsible for modifying any GTFS static schedule tables after
    a new schedule as been loaded

    currently, we are only addding pre-computed metrics values to the static_stop_times tables
    """
    load_temp_table_headway_gen(static_version_key, db_manager)
    generate_scheduled_travel_times(static_version_key, db_manager)
    generate_scheduled_branch_headways(db_manager)
    generate_scheduled_trunk_headways(db_manager)
