from typing import List

import sqlalchemy as sa
from lamp_py.postgres.postgres_schema import (
    ServiceIdDates,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    VehicleEvents,
    VehicleTrips,
    StaticRoutes,
)


def get_static_trips_cte(
    version_keys: List[int], service_date: int
) -> sa.sql.selectable.CTE:
    """
    return CTE named "static_trip_cte" representing all static trips on given service date

    a "set" of static trips will be returned for every "static_version_key" key value.

    created fields to be returnted:
        - static_trip_first_stop (bool indicating first stop of trip)
        - static_trip_last_stop (bool indicating last stop of trip)
        - static_stop_rank (rank field counting from 1 to N number of stops on trip)
    """
    return (
        sa.select(
            StaticStopTimes.static_version_key,
            StaticStopTimes.trip_id.label("static_trip_id"),
            StaticStopTimes.arrival_time.label("static_stop_timestamp"),
            sa.func.coalesce(
                StaticStops.parent_station,
                StaticStops.stop_id,
            ).label("parent_station"),
            (
                sa.func.lag(StaticStopTimes.departure_time)
                .over(
                    partition_by=(
                        StaticStopTimes.static_version_key,
                        StaticStopTimes.trip_id,
                    ),
                    order_by=StaticStopTimes.stop_sequence,
                )
                .is_(None)
            ).label("static_trip_first_stop"),
            (
                sa.func.lead(StaticStopTimes.departure_time)
                .over(
                    partition_by=(
                        StaticStopTimes.static_version_key,
                        StaticStopTimes.trip_id,
                    ),
                    order_by=StaticStopTimes.stop_sequence,
                )
                .is_(None)
            ).label("static_trip_last_stop"),
            sa.func.rank()
            .over(
                partition_by=(
                    StaticStopTimes.static_version_key,
                    StaticStopTimes.trip_id,
                ),
                order_by=StaticStopTimes.stop_sequence,
            )
            .label("static_trip_stop_rank"),
            StaticTrips.route_id,
            StaticTrips.branch_route_id,
            StaticTrips.trunk_route_id,
            StaticTrips.direction_id,
        )
        .select_from(StaticStopTimes)
        .join(
            StaticTrips,
            sa.and_(
                StaticStopTimes.static_version_key
                == StaticTrips.static_version_key,
                StaticStopTimes.trip_id == StaticTrips.trip_id,
            ),
        )
        .join(
            StaticStops,
            sa.and_(
                StaticStopTimes.static_version_key
                == StaticStops.static_version_key,
                StaticStopTimes.stop_id == StaticStops.stop_id,
            ),
        )
        .join(
            ServiceIdDates,
            sa.and_(
                StaticStopTimes.static_version_key
                == ServiceIdDates.static_version_key,
                StaticTrips.service_id == ServiceIdDates.service_id,
                StaticTrips.route_id == ServiceIdDates.route_id,
            ),
        )
        .join(
            StaticRoutes,
            sa.and_(
                StaticStopTimes.static_version_key
                == StaticRoutes.static_version_key,
                StaticTrips.route_id == StaticRoutes.route_id,
            ),
        )
        .where(
            StaticStopTimes.static_version_key.in_(version_keys),
            StaticRoutes.route_type != 3,
            ServiceIdDates.service_date == int(service_date),
        )
        .cte(name="static_trips_cte")
    )


def get_rt_trips_cte(service_date: int) -> sa.sql.selectable.CTE:
    """
    return CTE named "rt_trips_cte" representing all RT trips on a given service date

    created fields to be returnted:
        - rt_trip_first_stop_flag (bool indicating first stop of trip by trip_hash)
        - rt_trip_last_stop_flag (bool indicating last stop of trip by trip_hash)
        - static_stop_rank (rank field counting from 1 to N number of stops on trip by trip_hash)
    """

    return (
        sa.select(
            VehicleTrips.static_version_key,
            VehicleTrips.direction_id,
            VehicleTrips.route_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
            VehicleTrips.service_date,
            VehicleTrips.start_time,
            VehicleTrips.vehicle_id,
            VehicleTrips.stop_count,
            VehicleTrips.static_trip_id_guess,
            VehicleEvents.trip_hash,
            VehicleEvents.stop_sequence,
            VehicleEvents.parent_station,
            VehicleEvents.trip_stop_hash,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleEvents.tu_stop_timestamp,
            (
                sa.func.lag(VehicleEvents.trip_hash)
                .over(
                    partition_by=(VehicleEvents.trip_hash),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_first_stop_flag"),
            (
                sa.func.lead(VehicleEvents.trip_hash)
                .over(
                    partition_by=(VehicleEvents.trip_hash),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_last_stop_flag"),
            sa.func.rank()
            .over(
                partition_by=(VehicleEvents.trip_hash),
                order_by=VehicleEvents.stop_sequence,
            )
            .label("rt_trip_stop_rank"),
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleTrips.trip_hash == VehicleEvents.trip_hash,
        )
        .where(
            VehicleTrips.service_date == service_date,
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
    ).cte(name="rt_trips_cte")


def get_trips_for_metrics(
    version_keys: List[int], service_date: int
) -> sa.sql.selectable.CTE:
    """
    return CTE named "trips_for_metrics" with fields needed to develop metrics tables

    will return one record for every trip_stop_hash on 'service_date'

    joins rt_trips_cte to VehicleTrips on trip_hash field

    then joins static_trips_cte on static_trip_id_guess, timestamp, parent_station and static_stop_rank,

    the join with satic_stop_rank is required for routes that may visit the same
    parent station more than once on the same route, I think this only occurs on
    bus routes, so we may be able to drop this for performance_manager
    """

    static_trips_cte = get_static_trips_cte(version_keys, service_date)
    rt_trips_cte = get_rt_trips_cte(service_date)

    return (
        sa.select(
            rt_trips_cte.c.trip_stop_hash,
            rt_trips_cte.c.static_version_key,
            rt_trips_cte.c.trip_hash,
            rt_trips_cte.c.direction_id,
            rt_trips_cte.c.route_id,
            rt_trips_cte.c.branch_route_id,
            rt_trips_cte.c.trunk_route_id,
            rt_trips_cte.c.stop_count,
            rt_trips_cte.c.start_time,
            rt_trips_cte.c.vehicle_id,
            rt_trips_cte.c.parent_station,
            rt_trips_cte.c.vp_move_timestamp.label("move_timestamp"),
            sa.func.coalesce(
                rt_trips_cte.c.vp_stop_timestamp,
                rt_trips_cte.c.tu_stop_timestamp,
            ).label("stop_timestamp"),
            sa.func.coalesce(
                rt_trips_cte.c.vp_move_timestamp,
                rt_trips_cte.c.vp_stop_timestamp,
                rt_trips_cte.c.tu_stop_timestamp,
            ).label("sort_timestamp"),
            sa.func.coalesce(
                static_trips_cte.c.static_trip_first_stop,
                rt_trips_cte.c.rt_trip_first_stop_flag,
            ).label("first_stop_flag"),
            sa.func.coalesce(
                static_trips_cte.c.static_trip_last_stop,
                rt_trips_cte.c.rt_trip_last_stop_flag,
            ).label("last_stop_flag"),
            sa.func.coalesce(
                static_trips_cte.c.static_trip_stop_rank,
                rt_trips_cte.c.rt_trip_stop_rank,
            ).label("stop_rank"),
            sa.func.lead(rt_trips_cte.c.vp_move_timestamp)
            .over(
                partition_by=rt_trips_cte.c.vehicle_id,
                order_by=sa.func.coalesce(
                    rt_trips_cte.c.vp_move_timestamp,
                    rt_trips_cte.c.vp_stop_timestamp,
                    rt_trips_cte.c.tu_stop_timestamp,
                ),
            )
            .label("next_station_move"),
        )
        .distinct(
            rt_trips_cte.c.trip_stop_hash,
        )
        .select_from(rt_trips_cte)
        .join(
            static_trips_cte,
            sa.and_(
                rt_trips_cte.c.static_trip_id_guess
                == static_trips_cte.c.static_trip_id,
                rt_trips_cte.c.static_version_key
                == static_trips_cte.c.static_version_key,
                rt_trips_cte.c.parent_station
                == static_trips_cte.c.parent_station,
                rt_trips_cte.c.rt_trip_stop_rank
                >= static_trips_cte.c.static_trip_stop_rank,
            ),
            isouter=True,
        )
        .order_by(
            rt_trips_cte.c.trip_stop_hash,
            static_trips_cte.c.static_trip_stop_rank,
        )
    ).cte(name="trip_for_metrics")
