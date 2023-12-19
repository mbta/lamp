import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import bindparam


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
from .gtfs_utils import start_timestamp_to_seconds
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


def update_start_times(db_manager: DatabaseManager) -> None:
    """
    Add in start times to trips that do not have one. For scheduled trips,
    look these up in the static schedule by finding the earliest Stop Time for
    that trip id. Added trips cannot be found in the static schedule, so use
    the earliest vehicle position information we have about them.
    """
    # reusable subquery to find new events associated with trips that do not
    # have start times.
    missing_start_times = (
        sa.select(TempEventCompare.pm_trip_id)
        .distinct()
        .join(
            VehicleTrips, VehicleTrips.pm_trip_id == TempEventCompare.pm_trip_id
        )
        .where(
            TempEventCompare.start_time.is_(None),
            VehicleTrips.start_time.is_(None),
        )
        .subquery("missing_start_times_sub")
    )

    # get the start times for scheduled trips from the static schedule. match
    # the trip id to a trip in the static schedule and find the earliest
    # departure for that trip. these times are formatted as seconds after
    # midnight, so a direct insertion with a subquery is ok
    schedule_start_times_sub = (
        sa.select(
            VehicleTrips.pm_trip_id,
            sa.func.min(StaticStopTimes.departure_time).label("start_time"),
        )
        .join(
            missing_start_times,
            missing_start_times.c.pm_trip_id == VehicleTrips.pm_trip_id,
        )
        .join(
            StaticStopTimes,
            VehicleTrips.trip_id == StaticStopTimes.trip_id,
            VehicleTrips.static_version_key
            == StaticStopTimes.static_version_key,
        )
        .group_by(
            VehicleTrips.pm_trip_id,
        )
        .subquery("scheduled_start_times")
    )

    schedule_start_times_update_query = (
        sa.update(VehicleTrips.__table__)
        .where(VehicleTrips.pm_trip_id == schedule_start_times_sub.c.pm_trip_id)
        .values(start_time=schedule_start_times_sub.c.start_time)
    )

    db_manager.execute(schedule_start_times_update_query)

    # get the start times for added trips. by definition, these will not be in
    # the static schedule, so find the earliest departure from the real time
    # feed for this trip. these times are formatted as a UTC timestamp, and
    # need to be converted into seconds after midnight. pull the values out as
    # a dataframe, apply the conversion, and load back into the database using
    # a binding.
    unscheduled_start_times = db_manager.select_as_dataframe(
        sa.select(
            VehicleEvents.pm_trip_id.label("b_pm_trip_id"),
            # we are choosing to define added trip start times as the time when
            # the train departs the first station. on occasion, a trip will not
            # have a move time. in those cases, use the earliest stop time.
            sa.func.coalesce(
                sa.func.min(VehicleEvents.vp_move_timestamp),
                sa.func.min(VehicleEvents.vp_stop_timestamp),
                sa.func.min(VehicleEvents.tu_stop_timestamp),
            ).label("b_start_time"),
        )
        .select_from(VehicleEvents)
        .join(
            missing_start_times,
            VehicleEvents.pm_trip_id == missing_start_times.c.pm_trip_id,
        )
        .group_by(
            VehicleEvents.pm_trip_id,
        )
    )

    if unscheduled_start_times.shape[0] > 0:
        unscheduled_start_times["b_start_time"] = (
            unscheduled_start_times["b_start_time"]
            .apply(start_timestamp_to_seconds)
            .astype("int64")
        )

        start_times_update_query = (
            sa.update(VehicleTrips.__table__)
            .where(VehicleTrips.pm_trip_id == bindparam("b_pm_trip_id"))
            .values(start_time=bindparam("b_start_time"))
        )

        db_manager.execute_with_data(
            start_times_update_query, unscheduled_start_times
        )


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
    Update
     - static_trip_id_guess
     - static_stop_count
     - static_start_time

    for trips with exact matches to static trips
    """
    static_stop_sub = (
        sa.select(
            StaticStopTimes.static_version_key,
            StaticStopTimes.trip_id,
            sa.func.count(StaticStopTimes.stop_sequence).label(
                "static_stop_count"
            ),
        )
        .where(
            StaticStopTimes.static_version_key
            == sa.func.any(
                sa.func.array(
                    sa.select(TempEventCompare.static_version_key)
                    .distinct()
                    .scalar_subquery()
                )
            )
        )
        .group_by(
            StaticStopTimes.static_version_key,
            StaticStopTimes.trip_id,
        )
        .subquery("static_stops_count")
    )

    rt_static_match_sub = (
        sa.select(
            TempEventCompare.pm_trip_id,
            TempEventCompare.trip_id,
            sa.true().label("first_last_station_match"),
            static_stop_sub.c.static_stop_count,
            TempEventCompare.start_time,
        )
        .select_from(TempEventCompare)
        .distinct()
        .join(
            StaticTrips,
            sa.and_(
                StaticTrips.static_version_key
                == TempEventCompare.static_version_key,
                StaticTrips.trip_id == TempEventCompare.trip_id,
            ),
        )
        .join(
            static_stop_sub,
            sa.and_(
                static_stop_sub.c.static_version_key
                == TempEventCompare.static_version_key,
                static_stop_sub.c.trip_id == TempEventCompare.trip_id,
            ),
        )
        .where(TempEventCompare.start_time.is_not(None))
        .subquery("exact_trip_id_matches")
    )

    update_query = (
        sa.update(VehicleTrips.__table__)
        .values(
            static_trip_id_guess=rt_static_match_sub.c.trip_id,
            first_last_station_match=rt_static_match_sub.c.first_last_station_match,
            static_stop_count=rt_static_match_sub.c.static_stop_count,
            static_start_time=rt_static_match_sub.c.start_time,
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


def update_stop_sequence(db_manager: DatabaseManager) -> None:
    """
    Update canonical_stop_sequence  and sync_stop_sequence from static_route_patterns
    """
    partition_by = (
        StaticRoutePatterns.static_version_key,
        StaticRoutePatterns.direction_id,
        sa.func.coalesce(
            StaticTrips.branch_route_id, StaticTrips.trunk_route_id
        ).label("route_id"),
    )
    static_canon = (
        sa.select(
            StaticRoutePatterns.direction_id,
            StaticTrips.trunk_route_id,
            sa.func.coalesce(
                StaticTrips.branch_route_id, StaticTrips.trunk_route_id
            ).label("route_id"),
            StaticStops.parent_station,
            sa.over(
                sa.func.row_number(),
                partition_by=partition_by,
                order_by=StaticStopTimes.stop_sequence,
            ).label("stop_sequence"),
            StaticRoutePatterns.static_version_key,
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
            StaticRoutePatterns.static_version_key
            == sa.func.any(
                sa.func.array(
                    sa.select(TempEventCompare.static_version_key)
                    .distinct()
                    .scalar_subquery()
                )
            ),
            StaticRoutePatterns.route_pattern_typicality == 1,
        )
        .subquery("static_canon")
    )

    rt_canon = (
        sa.select(
            VehicleEvents.pm_event_id,
            static_canon.c.stop_sequence,
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id,
        )
        .join(
            static_canon,
            sa.and_(
                VehicleTrips.direction_id == static_canon.c.direction_id,
                sa.func.coalesce(
                    VehicleTrips.branch_route_id,
                    VehicleTrips.trunk_route_id,
                )
                == static_canon.c.route_id,
                VehicleTrips.static_version_key
                == static_canon.c.static_version_key,
                VehicleEvents.parent_station == static_canon.c.parent_station,
            ),
        )
        .where(
            VehicleEvents.service_date
            == sa.func.any(
                sa.func.array(
                    sa.select(TempEventCompare.service_date)
                    .distinct()
                    .scalar_subquery()
                )
            ),
        )
        .subquery("rt_canon")
    )

    update_rt_canon = (
        sa.update(VehicleEvents.__table__)
        .values(
            canonical_stop_sequence=rt_canon.c.stop_sequence,
        )
        .where(
            VehicleEvents.pm_event_id == rt_canon.c.pm_event_id,
        )
    )

    process_logger = ProcessLogger("l1_events.update_canonical_stop_sequence")
    process_logger.log_start()
    db_manager.execute(update_rt_canon)
    process_logger.log_complete()

    # select "zero_point" parent_stations
    # this query will produce one parent_station for each trunk_route_id that
    # is the most likey to have all branch_routes passing through them
    zero_point_stop = (
        sa.select(
            static_canon.c.trunk_route_id,
            static_canon.c.parent_station,
            sa.literal(0).label("sync_start"),
        )
        .distinct(
            static_canon.c.trunk_route_id,
        )
        .group_by(
            static_canon.c.trunk_route_id,
            static_canon.c.parent_station,
        )
        .order_by(
            static_canon.c.trunk_route_id,
            sa.func.count(
                static_canon.c.stop_sequence,
            ).desc(),
            (
                sa.func.max(static_canon.c.stop_sequence)
                - sa.func.min(static_canon.c.stop_sequence)
            ).desc(),
        )
        .subquery("zero_points")
    )

    # select stop_sequence number for the zerp_point parent_station of each route-branch,
    # consider this value the stop_sequence "adjustment" value
    zero_seq_vals = (
        sa.select(
            static_canon.c.direction_id,
            static_canon.c.route_id,
            static_canon.c.stop_sequence.label("seq_adjust"),
        )
        .select_from(static_canon)
        .join(
            zero_point_stop,
            sa.and_(
                zero_point_stop.c.trunk_route_id
                == static_canon.c.trunk_route_id,
                zero_point_stop.c.parent_station
                == static_canon.c.parent_station,
            ),
        )
        .subquery("zero_seq_vals")
    )

    # select the minimum stop_sequence value and minimum difference
    # between a stop_sequence and stop_sequence "adjustment" for each branch-route
    # these values will be used to normalize canonical stop_sequence values across a trunk
    sync_adjust_vals = (
        sa.select(
            static_canon.c.direction_id,
            static_canon.c.trunk_route_id,
            sa.func.min(static_canon.c.stop_sequence).label("min_seq"),
            sa.func.min(
                static_canon.c.stop_sequence - zero_seq_vals.c.seq_adjust
            ).label("min_sync"),
        )
        .select_from(static_canon)
        .join(
            zero_seq_vals,
            sa.and_(
                zero_seq_vals.c.direction_id == static_canon.c.direction_id,
                zero_seq_vals.c.route_id == static_canon.c.route_id,
            ),
        )
        .group_by(
            static_canon.c.direction_id,
            static_canon.c.trunk_route_id,
        )
        .subquery("sync_adjust_vals")
    )

    # create sync_stop_sequence
    # canonical_stop_sequence - zero_parent_stop_sequence - minimum_sync_sequence_adjustment(for trunk) + minimum_canonical_stop_sequence(for trunk)
    sync_values = (
        sa.select(
            static_canon.c.direction_id,
            static_canon.c.route_id,
            static_canon.c.parent_station,
            static_canon.c.static_version_key,
            (
                static_canon.c.stop_sequence
                - zero_seq_vals.c.seq_adjust
                - sync_adjust_vals.c.min_sync
                + sync_adjust_vals.c.min_seq
            ).label("sync_stop_sequence"),
        )
        .select_from(static_canon)
        .join(
            zero_seq_vals,
            sa.and_(
                zero_seq_vals.c.direction_id == static_canon.c.direction_id,
                zero_seq_vals.c.route_id == static_canon.c.route_id,
            ),
        )
        .join(
            sync_adjust_vals,
            sa.and_(
                sync_adjust_vals.c.direction_id == static_canon.c.direction_id,
                sync_adjust_vals.c.trunk_route_id
                == static_canon.c.trunk_route_id,
            ),
        )
        .subquery(("sync_values"))
    )

    rt_sync = (
        sa.select(
            VehicleEvents.pm_event_id,
            sync_values.c.sync_stop_sequence,
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id,
        )
        .join(
            sync_values,
            sa.and_(
                VehicleTrips.direction_id == sync_values.c.direction_id,
                sa.func.coalesce(
                    VehicleTrips.branch_route_id,
                    VehicleTrips.trunk_route_id,
                )
                == sync_values.c.route_id,
                VehicleTrips.static_version_key
                == sync_values.c.static_version_key,
                VehicleEvents.parent_station == sync_values.c.parent_station,
            ),
        )
        .where(
            VehicleEvents.service_date
            == sa.func.any(
                sa.func.array(
                    sa.select(TempEventCompare.service_date)
                    .distinct()
                    .scalar_subquery()
                )
            ),
        )
        .subquery("rt_sync")
    )

    update_rt_sync = (
        sa.update(VehicleEvents.__table__)
        .values(
            sync_stop_sequence=rt_sync.c.sync_stop_sequence,
        )
        .where(
            VehicleEvents.pm_event_id == rt_sync.c.pm_event_id,
        )
    )

    process_logger = ProcessLogger("l1_events.update_sync_stop_sequence")
    process_logger.log_start()
    db_manager.execute(update_rt_sync)
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
    update_start_times(db_manager)
    update_directions(db_manager)
    update_stop_sequence(db_manager)
    update_backup_static_trip_id(db_manager)
