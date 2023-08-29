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


def static_trips_subquery(
    static_version_key: int, service_date: int
) -> sa.sql.selectable.Subquery:
    """
    return Selectable representing all static trips on
    given service_date and static_version_key value combo

    created fields to be returned:
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
            StaticStopTimes.static_version_key == int(static_version_key),
            StaticTrips.static_version_key == int(static_version_key),
            StaticStops.static_version_key == int(static_version_key),
            ServiceIdDates.static_version_key == int(static_version_key),
            StaticRoutes.static_version_key == int(static_version_key),
            StaticRoutes.route_type != 3,
            ServiceIdDates.service_date == int(service_date),
        )
        .subquery(name="static_trips_sub")
    )


def rt_trips_subquery(service_date: int) -> sa.sql.selectable.Subquery:
    """
    return Selectable representing all RT trips on a given service date

    created fields to be returned:
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
            VehicleEvents.pm_trip_id,
            VehicleEvents.stop_sequence,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleEvents.tu_stop_timestamp,
            (
                sa.func.lag(VehicleEvents.pm_trip_id)
                .over(
                    partition_by=(VehicleEvents.pm_trip_id),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_first_stop_flag"),
            (
                sa.func.lead(VehicleEvents.pm_trip_id)
                .over(
                    partition_by=(VehicleEvents.pm_trip_id),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_last_stop_flag"),
            sa.func.rank()
            .over(
                partition_by=(VehicleEvents.pm_trip_id),
                order_by=VehicleEvents.stop_sequence,
            )
            .label("rt_trip_stop_rank"),
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleTrips.pm_trip_id == VehicleEvents.pm_trip_id,
        )
        .where(
            VehicleTrips.service_date == service_date,
            VehicleEvents.service_date == service_date,
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
    ).subquery(name="rt_trips_sub")


def trips_for_metrics_subquery(
    static_version_key: int, service_date: int
) -> sa.sql.selectable.Subquery:
    """
    return Selectable named "trips_for_metrics" with fields needed to develop metrics tables

    will return one record for every unique trip-stop on 'service_date'

    joins rt_trips_sub to static_trips_sub on static_trip_id_guess, static_version_key, parent_station and static_stop_rank,

    the join with static_stop_rank is required for routes that may visit the same
    parent station more than once on the same route, I think this only occurs on
    bus routes, so we may be able to drop this for performance_manager
    """

    static_trips_sub = static_trips_subquery(static_version_key, service_date)
    rt_trips_sub = rt_trips_subquery(service_date)

    return (
        sa.select(
            rt_trips_sub.c.static_version_key,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.direction_id,
            rt_trips_sub.c.route_id,
            rt_trips_sub.c.branch_route_id,
            rt_trips_sub.c.trunk_route_id,
            rt_trips_sub.c.stop_count,
            rt_trips_sub.c.start_time,
            rt_trips_sub.c.vehicle_id,
            rt_trips_sub.c.parent_station,
            rt_trips_sub.c.vp_move_timestamp.label("move_timestamp"),
            sa.func.coalesce(
                rt_trips_sub.c.vp_stop_timestamp,
                rt_trips_sub.c.tu_stop_timestamp,
            ).label("stop_timestamp"),
            sa.func.coalesce(
                rt_trips_sub.c.vp_move_timestamp,
                rt_trips_sub.c.vp_stop_timestamp,
                rt_trips_sub.c.tu_stop_timestamp,
            ).label("sort_timestamp"),
            sa.func.coalesce(
                static_trips_sub.c.static_trip_first_stop,
                rt_trips_sub.c.rt_trip_first_stop_flag,
            ).label("first_stop_flag"),
            sa.func.coalesce(
                static_trips_sub.c.static_trip_last_stop,
                rt_trips_sub.c.rt_trip_last_stop_flag,
            ).label("last_stop_flag"),
            sa.func.coalesce(
                static_trips_sub.c.static_trip_stop_rank,
                rt_trips_sub.c.rt_trip_stop_rank,
            ).label("stop_rank"),
            sa.func.lead(rt_trips_sub.c.vp_move_timestamp)
            .over(
                partition_by=rt_trips_sub.c.vehicle_id,
                order_by=rt_trips_sub.c.vp_move_timestamp,
            )
            .label("next_station_move"),
        )
        .distinct(
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
        .select_from(rt_trips_sub)
        .join(
            static_trips_sub,
            sa.and_(
                rt_trips_sub.c.static_trip_id_guess
                == static_trips_sub.c.static_trip_id,
                rt_trips_sub.c.static_version_key
                == static_trips_sub.c.static_version_key,
                rt_trips_sub.c.parent_station
                == static_trips_sub.c.parent_station,
                rt_trips_sub.c.rt_trip_stop_rank
                >= static_trips_sub.c.static_trip_stop_rank,
            ),
            isouter=True,
        )
        .order_by(
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
            static_trips_sub.c.static_trip_stop_rank,
        )
    ).subquery(name="trip_for_metrics")


def trips_for_headways_subquery(
    service_date: int,
) -> sa.sql.selectable.Subquery:
    """
    return Selectable named "trip_for_headways" with fields needed to develop headways values

    will return one record for every unique trip-stop on 'service_date'
    """

    rt_trips_sub = rt_trips_subquery(service_date)

    return (
        sa.select(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.direction_id,
            rt_trips_sub.c.route_id,
            rt_trips_sub.c.branch_route_id,
            rt_trips_sub.c.trunk_route_id,
            rt_trips_sub.c.parent_station,
            rt_trips_sub.c.stop_count,
            rt_trips_sub.c.vehicle_id,
            rt_trips_sub.c.vp_move_timestamp.label("move_timestamp"),
            sa.func.lead(rt_trips_sub.c.vp_move_timestamp)
            .over(
                partition_by=rt_trips_sub.c.vehicle_id,
                order_by=rt_trips_sub.c.vp_move_timestamp,
            )
            .label("next_station_move"),
        )
        .distinct(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
        .select_from(rt_trips_sub)
        .where(
            # drop trips with one stop count, probably not valid
            rt_trips_sub.c.stop_count
            > 1,
        )
        .order_by(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
    ).subquery(name="trip_for_headways")
