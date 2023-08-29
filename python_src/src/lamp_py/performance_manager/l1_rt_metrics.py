import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleEvents,
    TempEventCompare,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import (
    trips_for_metrics_subquery,
    trips_for_headways_subquery,
)


# pylint: disable=R0914
# pylint too many local variables (more than 15)
def update_metrics_columns(
    db_manager: DatabaseManager,
    seed_service_date: int,
    static_version_key: int,
) -> None:
    """
    update metrics columns in vehicle_events table for seed_service_date, static_version_key combination

    """

    process_logger = ProcessLogger("l1_rt_metrics_table_loader")
    process_logger.log_start()

    trips_for_metrics = trips_for_metrics_subquery(
        static_version_key, seed_service_date
    )
    trips_for_headways = trips_for_headways_subquery(
        service_date=seed_service_date,
    )

    # travel_times calculation:
    # limited to records where stop_timestamp > move_timestamp to avoid negative travel times
    # limited to non NULL stop and move timestamps to avoid NULL results
    # negative travel times are error records, should flag???
    update_travel_times = (
        sa.update(VehicleEvents.__table__)
        .values(
            travel_time_seconds=trips_for_metrics.c.stop_timestamp
            - trips_for_metrics.c.move_timestamp
        )
        .where(
            VehicleEvents.pm_trip_id == trips_for_metrics.c.pm_trip_id,
            VehicleEvents.service_date == trips_for_metrics.c.service_date,
            VehicleEvents.parent_station == trips_for_metrics.c.parent_station,
            trips_for_metrics.c.first_stop_flag == sa.false(),
            trips_for_metrics.c.stop_timestamp.is_not(None),
            trips_for_metrics.c.move_timestamp.is_not(None),
            trips_for_metrics.c.stop_timestamp
            > trips_for_metrics.c.move_timestamp,
        )
    )

    db_manager.execute(update_travel_times)

    # dwell_times calculations are different for the first stop of a trip
    # the first stop of a trip includes the dwell time since the stop_timestamp of
    # the end of the previous trip going in the opposite direction at that station
    #
    # all remaining dwell_times calculations are based on subtracting the a stop_timestamp
    # of a vehicle from the next move_timestamp of a vehicle
    #
    # the where statement of this query filters out any vehicle trips comprised of only 1 stop
    # on the assumption that those are not valid trips to include in the metrics calculations
    #
    # for consecutive trips that do not have same vehicle ID the first_stop headway
    # logic could have issues
    t_dwell_times_sub = (
        sa.select(
            trips_for_metrics.c.pm_trip_id,
            trips_for_metrics.c.service_date,
            trips_for_metrics.c.parent_station,
            sa.case(
                [
                    (
                        sa.and_(
                            trips_for_metrics.c.last_stop_flag == sa.false(),
                            trips_for_metrics.c.first_stop_flag == sa.false(),
                        ),
                        sa.func.lead(
                            trips_for_metrics.c.move_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        )
                        - trips_for_metrics.c.stop_timestamp,
                    ),
                    (
                        trips_for_metrics.c.first_stop_flag == sa.true(),
                        sa.func.lead(
                            trips_for_metrics.c.move_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        )
                        - sa.func.lag(
                            trips_for_metrics.c.stop_timestamp,
                        ).over(
                            partition_by=trips_for_metrics.c.vehicle_id,
                            order_by=trips_for_metrics.c.sort_timestamp,
                        ),
                    ),
                ],
                else_=sa.literal(None),
            ).label("dwell_time_seconds"),
        )
        .where(
            sa.or_(
                trips_for_metrics.c.stop_count > 1,
                trips_for_metrics.c.first_stop_flag == sa.false(),
            ),
        )
        .subquery(name="t_dwell_times")
    )

    # limit dwell times calculations to NON-NULL positive integers
    # would be nice if this could be done in the first query, but I can't
    # get it to work with sqlalchemy
    update_dwell_times = (
        sa.update(VehicleEvents.__table__)
        .values(
            dwell_time_seconds=t_dwell_times_sub.c.dwell_time_seconds,
        )
        .where(
            VehicleEvents.pm_trip_id == t_dwell_times_sub.c.pm_trip_id,
            VehicleEvents.service_date == t_dwell_times_sub.c.service_date,
            VehicleEvents.parent_station == t_dwell_times_sub.c.parent_station,
            t_dwell_times_sub.c.dwell_time_seconds.is_not(None),
            t_dwell_times_sub.c.dwell_time_seconds > 0,
        )
    )
    db_manager.execute(update_dwell_times)

    # this headways calculation is incomplete
    #
    # trunk and branch headways are the same except for one is partitioned on
    # trunk_route_id and the later on branch_route_id.
    #
    # headways are calculated with stop_timestamp to stop_timestamp for the
    # next station in a trip
    t_headways_branch_sub = (
        sa.select(
            trips_for_headways.c.pm_trip_id,
            trips_for_headways.c.service_date,
            trips_for_headways.c.parent_station,
            (
                trips_for_headways.c.next_station_move
                - sa.func.lag(
                    trips_for_headways.c.next_station_move,
                ).over(
                    partition_by=(
                        trips_for_headways.c.parent_station,
                        trips_for_headways.c.branch_route_id,
                        trips_for_headways.c.direction_id,
                    ),
                    order_by=trips_for_headways.c.next_station_move,
                )
            ).label("headway_branch_seconds"),
        )
        .where(
            trips_for_headways.c.branch_route_id.is_not(None),
        )
        .subquery(name="t_headways_branch")
    )

    # limit headways branch calculations to NON-NULL positive integers
    # would be nice if this could be done in the first query, but I can't
    # get it to work with sqlalchemy
    update_branch_headways = (
        sa.update(VehicleEvents.__table__)
        .values(
            headway_branch_seconds=t_headways_branch_sub.c.headway_branch_seconds
        )
        .where(
            VehicleEvents.pm_trip_id == t_headways_branch_sub.c.pm_trip_id,
            VehicleEvents.service_date == t_headways_branch_sub.c.service_date,
            VehicleEvents.parent_station
            == t_headways_branch_sub.c.parent_station,
            t_headways_branch_sub.c.headway_branch_seconds.is_not(None),
            t_headways_branch_sub.c.headway_branch_seconds > 0,
        )
    )
    db_manager.execute(update_branch_headways)

    t_headways_trunk_sub = (
        sa.select(
            trips_for_headways.c.pm_trip_id,
            trips_for_headways.c.service_date,
            trips_for_headways.c.parent_station,
            (
                trips_for_headways.c.next_station_move
                - sa.func.lag(
                    trips_for_headways.c.next_station_move,
                ).over(
                    partition_by=(
                        trips_for_headways.c.parent_station,
                        trips_for_headways.c.trunk_route_id,
                        trips_for_headways.c.direction_id,
                    ),
                    order_by=trips_for_headways.c.next_station_move,
                )
            ).label("headway_trunk_seconds"),
        )
        .where(
            trips_for_headways.c.trunk_route_id.is_not(None),
        )
        .subquery(name="t_headways_trunk")
    )

    # limit headways trunk calculations to NON-NULL positive integers
    # would be nice if this could be done in the first CTE, but I can't
    # get it to work with sqlalchemy
    update_trunk_headways = (
        sa.update(VehicleEvents.__table__)
        .values(
            headway_trunk_seconds=t_headways_trunk_sub.c.headway_trunk_seconds
        )
        .where(
            VehicleEvents.pm_trip_id == t_headways_trunk_sub.c.pm_trip_id,
            VehicleEvents.service_date == t_headways_trunk_sub.c.service_date,
            VehicleEvents.parent_station
            == t_headways_trunk_sub.c.parent_station,
            t_headways_trunk_sub.c.headway_trunk_seconds.is_not(None),
            t_headways_trunk_sub.c.headway_trunk_seconds > 0,
        )
    )
    db_manager.execute(update_trunk_headways)

    process_logger.log_complete()


# pylint: enable=R0914


def update_metrics_from_temp_events(db_manager: DatabaseManager) -> None:
    """
    update daily metrics values for service_date, static_version_key combos in
    temp_event_compare table
    """
    service_date_query = sa.select(
        TempEventCompare.service_date,
        TempEventCompare.static_version_key,
    ).distinct()

    for result in db_manager.select_as_list(service_date_query):
        service_date = int(result["service_date"])
        static_version_key = int(result["static_version_key"])
        update_metrics_columns(
            db_manager=db_manager,
            seed_service_date=service_date,
            static_version_key=static_version_key,
        )
