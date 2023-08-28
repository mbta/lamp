import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    StaticTrips,
    TempEventCompare,
    StaticDirections,
    StaticStopTimes,
    StaticRoutePatterns,
    StaticStops,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from .l1_cte_statements import (
    static_trips_subquery,
)


def update_prev_next_trip_stop(db_manager: DatabaseManager) -> None:
    """
    Update `previous_trip_stop_pk_id` and `next_trip_stop_pk_id` fields in `vehicle_events` table
    """
    temp_trips = (
        sa.select(
            TempEventCompare.pm_trip_id,
        )
        .distinct()
        .subquery()
    )

    prev_next_trip_stop_sub = (
        sa.select(
            VehicleEvents.pm_event_id,
            sa.func.lead(VehicleEvents.pm_event_id)
            .over(
                partition_by=VehicleEvents.pm_trip_id,
                order_by=VehicleEvents.stop_sequence,
            )
            .label("next_trip_stop_pk_id"),
            sa.func.lag(VehicleEvents.pm_event_id)
            .over(
                partition_by=VehicleEvents.pm_trip_id,
                order_by=VehicleEvents.stop_sequence,
            )
            .label("previous_trip_stop_pk_id"),
        ).join(
            temp_trips,
            temp_trips.c.pm_trip_id == VehicleEvents.pm_trip_id,
        )
    ).subquery(name="prev_next_trip_stops")

    update_query = (
        sa.update(VehicleEvents.__table__)
        .where(
            VehicleEvents.pm_event_id == prev_next_trip_stop_sub.c.pm_event_id,
        )
        .values(
            next_trip_stop_pm_event_id=prev_next_trip_stop_sub.c.next_trip_stop_pk_id,
            previous_trip_stop_pm_event_id=prev_next_trip_stop_sub.c.previous_trip_stop_pk_id,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_prev_next_trip_stop")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def load_new_trip_data(db_manager: DatabaseManager) -> None:
    """
    INSERT / UPDATE "vehicle_trip" table

    This guarantees that all events will have
    matching trips data in the "vehicle_trips" table

    This INSERT/UPDATE logic will load distinct trip information from the last
    recorded trip-stop event of a trip. The information in the last trip-stop
    event is assumed to be more accurate than information from the first
    trip-stop event because some values can carry over from the last trip of the
    vehicle. The `TempEventCompare.stop_sequence.desc()` `order_by` call is
    responsible for this behavior.
    """
    process_logger = ProcessLogger("l1_trips.load_new_trips")
    process_logger.log_start()

    distinct_trip_records = (
        sa.select(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.direction_id,
            TempEventCompare.start_time,
            TempEventCompare.vehicle_id,
            TempEventCompare.trip_id,
            TempEventCompare.vehicle_label,
            TempEventCompare.vehicle_consist,
            TempEventCompare.static_version_key,
        )
        .distinct(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.trip_id,
        )
        .order_by(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.trip_id,
            TempEventCompare.stop_sequence.desc(),
        )
    )

    trip_insert_columns = [
        "service_date",
        "route_id",
        "direction_id",
        "start_time",
        "vehicle_id",
        "trip_id",
        "vehicle_label",
        "vehicle_consist",
        "static_version_key",
    ]

    trip_insert_query = (
        postgresql.insert(VehicleTrips.__table__)
        .from_select(trip_insert_columns, distinct_trip_records)
        .on_conflict_do_nothing(
            index_elements=[
                VehicleTrips.service_date,
                VehicleTrips.route_id,
                VehicleTrips.trip_id,
            ],
        )
    )
    db_manager.execute(trip_insert_query)

    distinct_update_query = (
        sa.select(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.direction_id,
            TempEventCompare.start_time,
            TempEventCompare.vehicle_id,
            TempEventCompare.trip_id,
            TempEventCompare.vehicle_label,
            TempEventCompare.vehicle_consist,
        )
        .distinct(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.trip_id,
        )
        .order_by(
            TempEventCompare.service_date,
            TempEventCompare.route_id,
            TempEventCompare.trip_id,
            TempEventCompare.stop_sequence.desc(),
        )
        .where(
            sa.or_(
                TempEventCompare.vp_move_timestamp.is_not(None),
                TempEventCompare.vp_stop_timestamp.is_not(None),
            )
        )
        .subquery(name="trip_update")
    )

    trip_update_query = (
        sa.update(VehicleTrips.__table__)
        .values(
            trip_id=distinct_update_query.c.trip_id,
            vehicle_label=distinct_update_query.c.vehicle_label,
            vehicle_consist=distinct_update_query.c.vehicle_consist,
        )
        .where(
            VehicleTrips.service_date == distinct_update_query.c.service_date,
            VehicleTrips.route_id == distinct_update_query.c.route_id,
            VehicleTrips.trip_id == distinct_update_query.c.trip_id,
        )
    )
    db_manager.execute(trip_update_query)

    process_logger.log_complete()


def update_static_version_key(db_manager: DatabaseManager) -> None:
    """
    Update static_version_key so each day only uses one key
    """
    version_key_sub = (
        sa.select(
            TempEventCompare.service_date,
            sa.func.max(TempEventCompare.static_version_key).label(
                "max_version_key"
            ),
        )
        .group_by(
            TempEventCompare.service_date,
        )
        .subquery("update_version_key")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .values(
            static_version_key=version_key_sub.c.max_version_key,
        )
        .where(
            VehicleTrips.pm_trip_id == version_key_sub.c.service_date,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_static_version_key")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_trip_stop_counts(db_manager: DatabaseManager) -> None:
    """
    Update "stop_count" field for trips with new events

    this function call should also update branch_route_id and trunk_route_id columns
    for the trips table by activiating the rt_trips_update_branch_trunk TDS trigger
    """
    distinct_trips = (
        sa.select(TempEventCompare.service_date, TempEventCompare.pm_trip_id)
        .distinct()
        .subquery("distinct_trips")
    )

    new_stop_counts_cte = (
        sa.select(
            VehicleEvents.pm_trip_id,
            sa.func.count(VehicleEvents.pm_trip_id).label("stop_count"),
        )
        .select_from(VehicleEvents)
        .join(
            distinct_trips,
            sa.and_(
                distinct_trips.c.service_date == VehicleEvents.service_date,
                distinct_trips.c.pm_trip_id == VehicleEvents.pm_trip_id,
            ),
        )
        .where(
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
        .group_by(
            VehicleEvents.pm_trip_id,
        )
        .subquery("new_stop_counts")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .values(stop_count=new_stop_counts_cte.c.stop_count)
        .where(
            VehicleTrips.pm_trip_id == new_stop_counts_cte.c.pm_trip_id,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_trip_stop_counts")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_static_trip_id_guess_exact(db_manager: DatabaseManager) -> None:
    """
    Update static_trip_id_guess with exact matches between rt and static trips
    """
    rt_static_match_sub = (
        sa.select(
            TempEventCompare.pm_trip_id,
            TempEventCompare.trip_id,
            sa.true().label("first_last_station_match"),
            # TODO: add stop_count from static pre-processing when available # pylint: disable=fixme
            # TODO: add start_time from sattic pre-processing when available # pylint: disable=fixme
        )
        .select_from(TempEventCompare)
        .distinct()
        .join(
            StaticTrips,
            sa.and_(
                StaticTrips.static_version_key
                == TempEventCompare.static_version_key,
                StaticTrips.trip_id == TempEventCompare.trip_id,
                StaticTrips.direction_id == TempEventCompare.direction_id,
                StaticTrips.route_id == TempEventCompare.route_id,
            ),
        )
        .subquery("exact_trip_id_matches")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .values(
            static_trip_id_guess=rt_static_match_sub.c.trip_id,
            first_last_station_match=rt_static_match_sub.c.first_last_station_match,
            # TODO: add stop_count from static pre-processing when available # pylint: disable=fixme
            # TODO: add start_time from sattic pre-processing when available # pylint: disable=fixme
        )
        .where(
            VehicleTrips.pm_trip_id == rt_static_match_sub.c.pm_trip_id,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_exact_trip_matches")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_directions(db_manager: DatabaseManager) -> None:
    """
    Update direction and direction_destination from static tables
    """
    temp_trips = (
        sa.select(
            TempEventCompare.pm_trip_id,
            TempEventCompare.direction_id,
            TempEventCompare.route_id,
            TempEventCompare.static_version_key,
        )
        .distinct()
        .subquery()
    )

    directions_sub = (
        sa.select(
            temp_trips.c.pm_trip_id,
            StaticDirections.direction,
            StaticDirections.direction_destination,
        )
        .select_from(temp_trips)
        .join(
            StaticDirections,
            sa.and_(
                temp_trips.c.static_version_key
                == StaticDirections.static_version_key,
                temp_trips.c.direction_id == StaticDirections.direction_id,
                temp_trips.c.route_id == StaticDirections.route_id,
            ),
        )
        .subquery("update_directions")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .values(
            direction=directions_sub.c.direction,
            direction_destination=directions_sub.c.direction_destination,
        )
        .where(
            VehicleTrips.pm_trip_id == directions_sub.c.pm_trip_id,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_directions")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


def update_canonical_stop_sequence(db_manager: DatabaseManager) -> None:
    """
    Update canonical_stop_sequence from static_route_patterns
    """
    select_update_params = sa.select(
        TempEventCompare.service_date,
        TempEventCompare.static_version_key,
    ).distinct()

    for result in db_manager.select_as_list(select_update_params):
        service_date = result["service_date"]
        version_key = result["static_version_key"]

        static_sub = (
            sa.select(
                StaticRoutePatterns.direction_id,
                sa.func.coalesce(
                    StaticTrips.branch_route_id, StaticTrips.trunk_route_id
                ).label("route_id"),
                StaticStops.parent_station,
                StaticStopTimes.stop_sequence,
            )
            .select_from(StaticRoutePatterns)
            .join(
                StaticTrips,
                sa.and_(
                    StaticRoutePatterns.representative_trip_id
                    == StaticTrips.trip_id,
                    StaticRoutePatterns.static_version_key
                    == StaticTrips.static_version_key,
                ),
            )
            .join(
                StaticStopTimes,
                sa.and_(
                    StaticRoutePatterns.representative_trip_id
                    == StaticStopTimes.trip_id,
                    StaticRoutePatterns.static_version_key
                    == StaticStopTimes.static_version_key,
                ),
            )
            .join(
                StaticStops,
                sa.and_(
                    StaticStopTimes.stop_id == StaticStops.stop_id,
                    StaticStopTimes.static_version_key
                    == StaticStops.static_version_key,
                ),
            )
            .where(
                StaticRoutePatterns.static_version_key == version_key,
                StaticRoutePatterns.route_pattern_typicality == 1,
                StaticStopTimes.static_version_key == version_key,
                StaticTrips.static_version_key == version_key,
                StaticStops.static_version_key == version_key,
            )
            .subquery("static_sub")
        )

        canonical_sub = (
            sa.select(
                VehicleEvents.pm_event_id,
                static_sub.c.stop_sequence,
            )
            .select_from(VehicleEvents)
            .join(
                VehicleTrips,
                VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id,
            )
            .join(
                static_sub,
                sa.and_(
                    VehicleTrips.direction_id == static_sub.c.direction_id,
                    sa.func.coalesce(
                        VehicleTrips.branch_route_id,
                        VehicleTrips.trunk_route_id,
                    )
                    == static_sub.c.route_id,
                    VehicleEvents.parent_station == static_sub.c.parent_station,
                ),
            )
            .where(
                VehicleEvents.service_date == service_date,
                VehicleTrips.static_version_key == version_key,
            )
            .subquery("canonical_sub")
        )

        update_query = (
            sa.update(VehicleEvents.__table__)
            .values(
                canonical_stop_sequence=canonical_sub.c.stop_sequence,
            )
            .where(
                VehicleEvents.pm_event_id == canonical_sub.c.pm_event_id,
            )
        )

    process_logger = ProcessLogger("l1_events.update_canonical_stop_sequence")
    process_logger.log_start()
    db_manager.execute(update_query)
    process_logger.log_complete()


# pylint: disable=R0914
# pylint too many local variables (more than 15)
def backup_rt_static_trip_match(
    db_manager: DatabaseManager,
    seed_service_date: int,
    static_version_key: int,
) -> None:
    """
    perform "backup" match of RT trips to Static schedule trip

    this matches an RT trip to a static trip with the same branch_route_id or trunk_route_id if branch is null
    and direction with the closest start_time
    """
    static_trips_sub = static_trips_subquery(
        static_version_key, seed_service_date
    )

    # to build a 'summary' trips table only the first and last records for each
    # static trip are needed.
    first_stop_static_sub = (
        sa.select(
            static_trips_sub.c.static_trip_id,
            static_trips_sub.c.static_stop_timestamp.label("static_start_time"),
        )
        .select_from(static_trips_sub)
        .where(static_trips_sub.c.static_trip_first_stop == sa.true())
        .subquery(name="first_stop_static_sub")
    )

    # join first_stop_static_sub with last stop records to create trip summary records
    static_trips_summary_sub = (
        sa.select(
            static_trips_sub.c.static_trip_id,
            sa.func.coalesce(
                static_trips_sub.c.branch_route_id,
                static_trips_sub.c.trunk_route_id,
            ).label("route_id"),
            static_trips_sub.c.direction_id,
            first_stop_static_sub.c.static_start_time,
            static_trips_sub.c.static_trip_stop_rank.label("static_stop_count"),
        )
        .select_from(static_trips_sub)
        .join(
            first_stop_static_sub,
            static_trips_sub.c.static_trip_id
            == first_stop_static_sub.c.static_trip_id,
        )
        .where(static_trips_sub.c.static_trip_last_stop == sa.true())
        .subquery(name="static_trips_summary_sub")
    )

    # pull RT trips records that are candidates for backup matching to static trips
    temp_trips = (
        sa.select(
            TempEventCompare.pm_trip_id,
        )
        .distinct()
        .subquery()
    )

    rt_trips_summary_sub = (
        sa.select(
            VehicleTrips.pm_trip_id,
            VehicleTrips.direction_id,
            sa.func.coalesce(
                VehicleTrips.branch_route_id, VehicleTrips.trunk_route_id
            ).label("route_id"),
            VehicleTrips.start_time,
        )
        .select_from(VehicleTrips)
        .join(
            temp_trips,
            temp_trips.c.pm_trip_id == VehicleTrips.pm_trip_id,
        )
        .where(
            VehicleTrips.service_date == int(seed_service_date),
            VehicleTrips.static_version_key == int(static_version_key),
            VehicleTrips.first_last_station_match == sa.false(),
        )
        .subquery("rt_trips_for_backup_match")
    )

    # backup matching logic, should match all remaining RT trips to static trips,
    # assuming that the route_id exists in the static schedule data
    backup_trips_match = (
        sa.select(
            rt_trips_summary_sub.c.pm_trip_id,
            static_trips_summary_sub.c.static_trip_id,
            static_trips_summary_sub.c.static_start_time,
            static_trips_summary_sub.c.static_stop_count,
            sa.literal(False).label("first_last_station_match"),
        )
        .distinct(
            rt_trips_summary_sub.c.pm_trip_id,
        )
        .select_from(rt_trips_summary_sub)
        .join(
            static_trips_summary_sub,
            sa.and_(
                rt_trips_summary_sub.c.direction_id
                == static_trips_summary_sub.c.direction_id,
                rt_trips_summary_sub.c.route_id
                == static_trips_summary_sub.c.route_id,
            ),
        )
        .order_by(
            rt_trips_summary_sub.c.pm_trip_id,
            sa.func.abs(
                rt_trips_summary_sub.c.start_time
                - static_trips_summary_sub.c.static_start_time
            ),
        )
    ).subquery(name="backup_trips_match")

    update_query = (
        sa.update(VehicleTrips.__table__)
        .where(
            VehicleTrips.pm_trip_id == backup_trips_match.c.pm_trip_id,
        )
        .values(
            static_trip_id_guess=backup_trips_match.c.static_trip_id,
            static_start_time=backup_trips_match.c.static_start_time,
            static_stop_count=backup_trips_match.c.static_stop_count,
            first_last_station_match=backup_trips_match.c.first_last_station_match,
        )
    )

    db_manager.execute(update_query)


def update_backup_static_trip_id(db_manager: DatabaseManager) -> None:
    """
    perform backup static_trip_id matching on events_trips table

    """
    process_logger = ProcessLogger("l1_trips.update_backup_trip_matches")
    process_logger.log_start()

    service_date_query = sa.select(
        TempEventCompare.service_date,
        TempEventCompare.static_version_key,
    ).distinct()

    for result in db_manager.select_as_list(service_date_query):
        service_date = int(result["service_date"])
        static_version_key = int(result["static_version_key"])
        backup_rt_static_trip_match(
            db_manager=db_manager,
            seed_service_date=service_date,
            static_version_key=static_version_key,
        )

    process_logger.log_complete()


def process_trips(db_manager: DatabaseManager) -> None:
    """
    update vehicle_trips table based on new events in temp_event_compare

    """
    update_static_version_key(db_manager)
    update_trip_stop_counts(db_manager)
    update_prev_next_trip_stop(db_manager)
    update_static_trip_id_guess_exact(db_manager)
    update_directions(db_manager)
    update_canonical_stop_sequence(db_manager)
    update_backup_static_trip_id(db_manager)
