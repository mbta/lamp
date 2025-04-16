from typing import Optional

import pandas
import sqlalchemy as sa
import polars as pl
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.functions import count


from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.postgres.rail_performance_manager_schema import (
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
    static_trips_subquery_pl,
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
            TempEventCompare.revenue,
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
        "revenue",
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
            TempEventCompare.revenue,
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
            revenue=distinct_update_query.c.revenue,
        )
        .where(
            VehicleTrips.service_date == distinct_update_query.c.service_date,
            VehicleTrips.route_id == distinct_update_query.c.route_id,
            VehicleTrips.trip_id == distinct_update_query.c.trip_id,
        )
    )
    db_manager.execute(trip_update_query, disable_trip_tigger=True)

    process_logger.log_complete()


def update_static_version_key(db_manager: DatabaseManager) -> None:
    """
    Update static_version_key so each day only uses one key
    """
    version_key_sub = (
        sa.select(
            TempEventCompare.service_date,
            sa.func.max(TempEventCompare.static_version_key).label("max_version_key"),
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
            VehicleTrips.service_date == version_key_sub.c.service_date,
        )
    )

    process_logger = ProcessLogger("l1_trips.update_static_version_key")
    process_logger.log_start()
    db_manager.execute(update_query, disable_trip_tigger=True)
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
        .join(VehicleTrips, VehicleTrips.pm_trip_id == TempEventCompare.pm_trip_id)
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
            sa.and_(
                VehicleTrips.trip_id == StaticStopTimes.trip_id,
                VehicleTrips.static_version_key == StaticStopTimes.static_version_key,
            ),
        )
        .group_by(
            VehicleTrips.pm_trip_id,
        )
        .subquery("scheduled_start_times")
    )

    schedule_start_times_update = (
        sa.update(VehicleTrips.__table__)
        .where(VehicleTrips.pm_trip_id == schedule_start_times_sub.c.pm_trip_id)
        .values(start_time=schedule_start_times_sub.c.start_time)
    )

    db_manager.execute(schedule_start_times_update, disable_trip_tigger=True)

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
            unscheduled_start_times["b_start_time"].apply(start_timestamp_to_seconds).astype("int64")
        )

        start_times_update_query = (
            sa.update(VehicleTrips.__table__)
            .where(VehicleTrips.pm_trip_id == bindparam("b_pm_trip_id"))
            .values(start_time=bindparam("b_start_time"))
        )

        db_manager.execute_with_data(
            start_times_update_query,
            unscheduled_start_times,
            disable_trip_tigger=True,
        )


def update_branch_trunk_route_id(db_manager: DatabaseManager) -> None:
    """
    update `branch_route_id` and `trunk_route_id` fields in trips table
    """
    distinct_t_trips = (
        sa.select(TempEventCompare.service_date, TempEventCompare.pm_trip_id).distinct().subquery("distinct_trips")
    )

    distinct_trips = (
        sa.select(
            VehicleTrips.pm_trip_id,
            VehicleTrips.route_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
        )
        .select_from(VehicleTrips)
        .join(
            distinct_t_trips,
            sa.and_(
                distinct_t_trips.c.service_date == VehicleTrips.service_date,
                distinct_t_trips.c.pm_trip_id == VehicleTrips.pm_trip_id,
            ),
        )
    )

    distinct_red_trips = (
        sa.select(TempEventCompare.service_date, TempEventCompare.pm_trip_id)
        .distinct()
        .where(TempEventCompare.route_id == "Red")
        .subquery("distinct_trips")
    )

    red_events = (
        sa.select(
            VehicleEvents.pm_trip_id,
            VehicleEvents.stop_id,
        )
        .select_from(VehicleEvents)
        .join(
            distinct_red_trips,
            sa.and_(
                distinct_red_trips.c.service_date == VehicleEvents.service_date,
                distinct_red_trips.c.pm_trip_id == VehicleEvents.pm_trip_id,
            ),
        )
    )

    process_logger = ProcessLogger("l1_trips.update_branch_trunk_route_id")
    process_logger.log_start()

    trips_df = db_manager.select_as_dataframe(distinct_trips)
    red_events_df = db_manager.select_as_dataframe(red_events)

    def get_red_branch(pm_trip_id: int) -> Optional[str]:
        ashmont_stop_ids = {
            "70087",
            "70088",
            "70089",
            "70090",
            "70091",
            "70092",
            "70093",
            "70094",
            "70085",
            "70086",
        }
        braintree_stop_ids = {
            "70097",
            "70098",
            "70099",
            "70100",
            "70101",
            "70102",
            "70103",
            "70104",
            "70105",
            "70095",
            "70096",
        }
        trip_stop_ids = set(red_events_df[red_events_df["pm_trip_id"] == pm_trip_id]["stop_id"])
        if trip_stop_ids & ashmont_stop_ids:
            return "Red-A"
        if trip_stop_ids & braintree_stop_ids:
            return "Red-B"
        return None

    def map_trunk_route_id(route_id: str) -> str:
        if str(route_id).startswith("Green"):
            return "Green"
        return route_id

    def apply_branch_route_id(record: pandas.Series) -> Optional[str]:
        if str(record["route_id"]).startswith("Green"):
            return record["route_id"]
        if str(record["route_id"]) == "Red":
            return get_red_branch(record["pm_trip_id"])
        return None

    trips_df["trunk_route_id"] = trips_df["route_id"].map(map_trunk_route_id)
    trips_df["branch_route_id"] = trips_df.apply(apply_branch_route_id, axis=1)

    trips_df = trips_df[
        [
            "pm_trip_id",
            "branch_route_id",
            "trunk_route_id",
        ]
    ].rename(
        columns={
            "pm_trip_id": "b_pm_trip_id",
            "branch_route_id": "b_branch_route_id",
            "trunk_route_id": "b_trunk_route_id",
        }
    )

    start_times_update_query = (
        sa.update(VehicleTrips.__table__)
        .where(VehicleTrips.pm_trip_id == bindparam("b_pm_trip_id"))
        .values(
            branch_route_id=bindparam("b_branch_route_id"),
            trunk_route_id=bindparam("b_trunk_route_id"),
        )
    )

    db_manager.execute_with_data(
        start_times_update_query,
        trips_df,
        disable_trip_tigger=True,
    )
    process_logger.log_complete()


def update_trip_stop_counts(db_manager: DatabaseManager) -> None:
    """
    Update "stop_count" field for trips with new events
    """
    distinct_trips = (
        sa.select(TempEventCompare.service_date, TempEventCompare.pm_trip_id).distinct().subquery("distinct_trips")
    )

    new_stop_counts_cte = (
        sa.select(
            VehicleEvents.pm_trip_id,
            count(VehicleEvents.pm_trip_id).label("stop_count"),
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
    db_manager.execute(update_query, disable_trip_tigger=True)
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
            count(StaticStopTimes.stop_sequence).label("static_stop_count"),
        )
        .where(
            StaticStopTimes.static_version_key
            == sa.func.any(sa.func.array(sa.select(TempEventCompare.static_version_key).distinct().scalar_subquery()))
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
                StaticTrips.static_version_key == TempEventCompare.static_version_key,
                StaticTrips.trip_id == TempEventCompare.trip_id,
            ),
        )
        .join(
            static_stop_sub,
            sa.and_(
                static_stop_sub.c.static_version_key == TempEventCompare.static_version_key,
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
    db_manager.execute(update_query, disable_trip_tigger=True)
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
                temp_trips.c.static_version_key == StaticDirections.static_version_key,
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
    db_manager.execute(update_query, disable_trip_tigger=True)
    process_logger.log_complete()


def update_stop_sequence(db_manager: DatabaseManager) -> None:
    """
    Update canonical_stop_sequence  and sync_stop_sequence from static_route_patterns
    """
    # select canonical trip_id for each trip_pattern and direction combination
    # this will first select any representative_trip_id where the route_pattern_typicality = 5
    # and then fall back to where the route_pattern_typicality = 1
    distinct_query = (
        sa.select(VehicleTrips.service_date, VehicleTrips.static_version_key)
        .distinct()
        .join(
            TempEventCompare,
            VehicleTrips.pm_trip_id == TempEventCompare.pm_trip_id,
        )
    )
    for record in db_manager.select_as_list(distinct_query):
        canon_trips = (
            sa.select(
                StaticRoutePatterns.direction_id,
                StaticRoutePatterns.representative_trip_id,
                StaticTrips.trunk_route_id,
                sa.func.coalesce(StaticTrips.branch_route_id, StaticTrips.trunk_route_id).label("route_id"),
                StaticRoutePatterns.static_version_key,
            )
            .distinct(
                sa.func.coalesce(StaticTrips.branch_route_id, StaticTrips.trunk_route_id),
                StaticRoutePatterns.direction_id,
                StaticRoutePatterns.static_version_key,
            )
            .select_from(StaticRoutePatterns)
            .join(
                StaticTrips,
                sa.and_(
                    StaticRoutePatterns.representative_trip_id == StaticTrips.trip_id,
                    StaticRoutePatterns.static_version_key == StaticTrips.static_version_key,
                ),
            )
            .where(
                StaticRoutePatterns.static_version_key == record["static_version_key"],
                sa.or_(
                    StaticRoutePatterns.route_pattern_typicality == 1,
                    StaticRoutePatterns.route_pattern_typicality == 5,
                ),
            )
            .order_by(
                sa.func.coalesce(StaticTrips.branch_route_id, StaticTrips.trunk_route_id),
                StaticRoutePatterns.direction_id,
                StaticRoutePatterns.static_version_key,
                StaticRoutePatterns.route_pattern_typicality.desc(),
            )
            .subquery("canon_trips")
        )
        # using the representative_trip_id's from the canon_trips query, create
        # stop_sequence values for each parent_station on each route in each direction.
        # stop_sequence's are created using the row_number function so that they
        # always start at 1 and increment according to the
        # StaticStopTimes.stop_sequence order
        static_canon = (
            sa.select(
                canon_trips.c.direction_id,
                canon_trips.c.trunk_route_id,
                canon_trips.c.route_id,
                StaticStops.parent_station,
                sa.over(
                    sa.func.row_number(),
                    partition_by=(
                        canon_trips.c.static_version_key,
                        canon_trips.c.direction_id,
                        canon_trips.c.route_id,
                    ),
                    order_by=StaticStopTimes.stop_sequence,
                ).label("stop_sequence"),
                canon_trips.c.static_version_key,
            )
            .select_from(canon_trips)
            .join(
                StaticStopTimes,
                sa.and_(
                    canon_trips.c.representative_trip_id == StaticStopTimes.trip_id,
                    canon_trips.c.static_version_key == StaticStopTimes.static_version_key,
                ),
            )
            .join(
                StaticStops,
                sa.and_(
                    StaticStopTimes.stop_id == StaticStops.stop_id,
                    StaticStopTimes.static_version_key == StaticStops.static_version_key,
                ),
            )
            .subquery("static_canon")
        )

        # subquery to join static_canon results to vehicle_events records
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
                    VehicleTrips.static_version_key == static_canon.c.static_version_key,
                    VehicleEvents.parent_station == static_canon.c.parent_station,
                ),
            )
            .where(
                VehicleEvents.service_date == record["service_date"],
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

        process_logger = ProcessLogger(
            "l1_events.update_canonical_stop_sequence",
            service_date=record["service_date"],
            static_version_key=record["static_version_key"],
        )
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
                count(
                    static_canon.c.stop_sequence,
                ).desc(),
                (sa.func.max(static_canon.c.stop_sequence) - sa.func.min(static_canon.c.stop_sequence)).desc(),
            )
            .subquery("zero_points")
        )

        # select stop_sequence number for the zero_point parent_station of each route-branch,
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
                    zero_point_stop.c.trunk_route_id == static_canon.c.trunk_route_id,
                    zero_point_stop.c.parent_station == static_canon.c.parent_station,
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
                sa.func.min(static_canon.c.stop_sequence - zero_seq_vals.c.seq_adjust).label("min_sync"),
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
        # sync_stop_sequence = canonical_stop_sequence - zero_parent_stop_sequence - minimum_sync_sequence_adjustment(for trunk) + minimum_canonical_stop_sequence(for trunk)
        # one sync_stop_sequence value is created for each trunk_route_id, direction_id, parent_station, static_version_key pair
        sync_values = (
            sa.select(
                static_canon.c.direction_id,
                static_canon.c.trunk_route_id,
                static_canon.c.parent_station,
                static_canon.c.static_version_key,
                (
                    static_canon.c.stop_sequence
                    - zero_seq_vals.c.seq_adjust
                    - sync_adjust_vals.c.min_sync
                    + sync_adjust_vals.c.min_seq
                ).label("sync_stop_sequence"),
            )
            .distinct()
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
                    sync_adjust_vals.c.trunk_route_id == static_canon.c.trunk_route_id,
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
                    VehicleTrips.trunk_route_id == sync_values.c.trunk_route_id,
                    VehicleTrips.static_version_key == sync_values.c.static_version_key,
                    VehicleEvents.parent_station == sync_values.c.parent_station,
                ),
            )
            .where(
                VehicleEvents.service_date == record["service_date"],
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

        process_logger = ProcessLogger(
            "l1_events.update_sync_stop_sequence",
            service_date=record["service_date"],
            static_version_key=record["static_version_key"],
        )
        process_logger.log_start()
        db_manager.execute(update_rt_sync)
        process_logger.log_complete()


def backup_trips_match_pl(rt_backup_trips: pl.DataFrame, static_trips: pl.DataFrame) -> pl.DataFrame:
    """
    Polars implementation of backup_trips_match subquery in backup_rt_static_trip_match
    """

    return rt_backup_trips.join_asof(
        static_trips,
        left_on="start_time",
        right_on="static_start_time",
        by=["direction_id", "route_id"],
        strategy="nearest",
        coalesce=True,
    ).select(
        "static_trip_id",
        "static_start_time",
        "static_stop_count",
        "pm_trip_id",
        pl.lit(False).alias("first_last_station_match"),
    )


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
    static_trips_df = static_trips_subquery_pl(seed_service_date)

    # pull RT trips records that are candidates for backup matching to static trips
    temp_trips = (
        sa.select(
            TempEventCompare.pm_trip_id,
        )
        .distinct()
        .subquery()
    )

    rt_trips_summary = (
        sa.select(
            VehicleTrips.pm_trip_id,
            VehicleTrips.direction_id,
            sa.func.coalesce(VehicleTrips.branch_route_id, VehicleTrips.trunk_route_id).label("route_id"),
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
    )

    rt_schema = {"pm_trip_id": pl.Int64, "direction_id": pl.Boolean, "route_id": pl.String, "start_time": pl.Int64}

    rt_trips_summary_df = pl.DataFrame(
        db_manager.select_as_list(rt_trips_summary, disable_trip_tigger=True), schema=rt_schema
    )

    # backup matching logic, should match all remaining RT trips to static trips,
    # assuming that the route_id exists in the static schedule data
    backup_trips_match_df = backup_trips_match_pl(rt_trips_summary_df, static_trips_df)

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

    db_manager.execute(update_query, disable_trip_tigger=True)


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
    update_branch_trunk_route_id(db_manager)
    update_trip_stop_counts(db_manager)
    update_prev_next_trip_stop(db_manager)
    update_static_trip_id_guess_exact(db_manager)
    update_start_times(db_manager)
    update_directions(db_manager)
    update_stop_sequence(db_manager)
    update_backup_static_trip_id(db_manager)
