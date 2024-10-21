"""dwell_time_update

Revision ID: 896dedd8a4db
Revises: 2dfbde5ec151
Create Date: 2024-07-03 08:53:01.675273

This change simplifies dwell_time calculations to not perform dwell time calculations
for stations at the start or end of a trip

Details
* upgrade -> iterate through service_date/static_version_key combos and update dwell_time

* downgrade -> Nothing

"""

from alembic import op
import sqlalchemy as sa

from lamp_py.runtime_utils.process_logger import ProcessLogger

# revision identifiers, used by Alembic.
revision = "896dedd8a4db"
down_revision = "2dfbde5ec151"
branch_labels = None
depends_on = None


def upgrade() -> None:
    date_query = sa.text("SELECT DISTINCT service_date, static_version_key FROM vehicle_trips ORDER BY service_date")

    conn = op.get_bind()
    result = conn.execute(date_query)
    for service_date, static_version_key in result.fetchall():
        stop_sync_update = f"""
            UPDATE
                vehicle_events
            SET
                dwell_time_seconds = t_dwell_times.dwell_time_seconds
            FROM
                (
                SELECT
                    trip_for_metrics.pm_trip_id AS pm_trip_id,
                    trip_for_metrics.service_date AS service_date,
                    trip_for_metrics.parent_station AS parent_station,
                    lead(trip_for_metrics.move_timestamp) OVER (PARTITION BY trip_for_metrics.pm_trip_id
                ORDER BY
                    trip_for_metrics.sort_timestamp) - trip_for_metrics.stop_timestamp AS dwell_time_seconds
                FROM
                    (
                    SELECT
                        DISTINCT ON
                        (rt_trips_sub.service_date,
                        rt_trips_sub.pm_trip_id,
                        rt_trips_sub.parent_station) rt_trips_sub.static_version_key AS static_version_key,
                        rt_trips_sub.pm_trip_id AS pm_trip_id,
                        rt_trips_sub.service_date AS service_date,
                        rt_trips_sub.direction_id AS direction_id,
                        rt_trips_sub.route_id AS route_id,
                        rt_trips_sub.branch_route_id AS branch_route_id,
                        rt_trips_sub.trunk_route_id AS trunk_route_id,
                        rt_trips_sub.stop_count AS stop_count,
                        rt_trips_sub.start_time AS start_time,
                        rt_trips_sub.vehicle_id AS vehicle_id,
                        rt_trips_sub.parent_station AS parent_station,
                        rt_trips_sub.vp_move_timestamp AS move_timestamp,
                        COALESCE(rt_trips_sub.vp_stop_timestamp,
                        rt_trips_sub.tu_stop_timestamp) AS stop_timestamp,
                        COALESCE(rt_trips_sub.vp_move_timestamp,
                        rt_trips_sub.vp_stop_timestamp,
                        rt_trips_sub.tu_stop_timestamp) AS sort_timestamp,
                        COALESCE(static_trips_sub.static_trip_first_stop,
                        rt_trips_sub.rt_trip_first_stop_flag) AS first_stop_flag,
                        COALESCE(static_trips_sub.static_trip_last_stop,
                        rt_trips_sub.rt_trip_last_stop_flag) AS last_stop_flag
                    FROM
                        (
                        SELECT
                            vehicle_trips.static_version_key AS static_version_key,
                            vehicle_trips.direction_id AS direction_id,
                            vehicle_trips.route_id AS route_id,
                            vehicle_trips.branch_route_id AS branch_route_id,
                            vehicle_trips.trunk_route_id AS trunk_route_id,
                            vehicle_trips.service_date AS service_date,
                            vehicle_trips.start_time AS start_time,
                            vehicle_trips.vehicle_id AS vehicle_id,
                            vehicle_trips.stop_count AS stop_count,
                            vehicle_trips.static_trip_id_guess AS static_trip_id_guess,
                            vehicle_events.pm_trip_id AS pm_trip_id,
                            vehicle_events.stop_sequence AS stop_sequence,
                            vehicle_events.parent_station AS parent_station,
                            vehicle_events.vp_move_timestamp AS vp_move_timestamp,
                            vehicle_events.vp_stop_timestamp AS vp_stop_timestamp,
                            vehicle_events.tu_stop_timestamp AS tu_stop_timestamp,
                            lag(vehicle_events.pm_trip_id) OVER (PARTITION BY vehicle_events.pm_trip_id
                        ORDER BY
                            vehicle_events.stop_sequence) IS NULL AS rt_trip_first_stop_flag,
                            lead(vehicle_events.pm_trip_id) OVER (PARTITION BY vehicle_events.pm_trip_id
                        ORDER BY
                            vehicle_events.stop_sequence) IS NULL AS rt_trip_last_stop_flag,
                            RANK() OVER (PARTITION BY vehicle_events.pm_trip_id
                        ORDER BY
                            vehicle_events.stop_sequence) AS rt_trip_stop_rank
                        FROM
                            vehicle_events
                        JOIN vehicle_trips ON
                            vehicle_trips.pm_trip_id = vehicle_events.pm_trip_id
                        WHERE
                            vehicle_trips.service_date = {service_date}
                            AND vehicle_events.service_date = {service_date}
                            AND (vehicle_events.vp_move_timestamp IS NOT NULL
                                OR vehicle_events.vp_stop_timestamp IS NOT NULL)) AS rt_trips_sub
                    LEFT OUTER JOIN (
                        SELECT
                            static_stop_times.static_version_key AS static_version_key,
                            static_stop_times.trip_id AS static_trip_id,
                            static_stop_times.arrival_time AS static_stop_timestamp,
                            COALESCE(static_stops.parent_station,
                            static_stops.stop_id) AS parent_station,
                            lag(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
                            static_stop_times.trip_id
                        ORDER BY
                            static_stop_times.stop_sequence) IS NULL AS static_trip_first_stop,
                            lead(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
                            static_stop_times.trip_id
                        ORDER BY
                            static_stop_times.stop_sequence) IS NULL AS static_trip_last_stop,
                            RANK() OVER (PARTITION BY static_stop_times.static_version_key,
                            static_stop_times.trip_id
                        ORDER BY
                            static_stop_times.stop_sequence) AS static_trip_stop_rank,
                            static_trips.route_id AS route_id,
                            static_trips.branch_route_id AS branch_route_id,
                            static_trips.trunk_route_id AS trunk_route_id,
                            static_trips.direction_id AS direction_id
                        FROM
                            static_stop_times
                        JOIN static_trips ON
                            static_stop_times.static_version_key = static_trips.static_version_key
                            AND static_stop_times.trip_id = static_trips.trip_id
                        JOIN static_stops ON
                            static_stop_times.static_version_key = static_stops.static_version_key
                            AND static_stop_times.stop_id = static_stops.stop_id
                        JOIN static_service_id_lookup ON
                            static_stop_times.static_version_key = static_service_id_lookup.static_version_key
                            AND static_trips.service_id = static_service_id_lookup.service_id
                            AND static_trips.route_id = static_service_id_lookup.route_id
                        JOIN static_routes ON
                            static_stop_times.static_version_key = static_routes.static_version_key
                            AND static_trips.route_id = static_routes.route_id
                        WHERE
                            static_stop_times.static_version_key = {static_version_key}
                            AND static_trips.static_version_key = {static_version_key}
                            AND static_stops.static_version_key = {static_version_key}
                            AND static_service_id_lookup.static_version_key = {static_version_key}
                            AND static_routes.static_version_key = {static_version_key}
                            AND static_routes.route_type != 3
                            AND static_service_id_lookup.service_date = {service_date}) AS static_trips_sub ON
                        rt_trips_sub.static_trip_id_guess = static_trips_sub.static_trip_id
                        AND rt_trips_sub.static_version_key = static_trips_sub.static_version_key
                        AND rt_trips_sub.parent_station = static_trips_sub.parent_station
                        AND rt_trips_sub.rt_trip_stop_rank >= static_trips_sub.static_trip_stop_rank
                    ORDER BY
                        rt_trips_sub.service_date,
                        rt_trips_sub.pm_trip_id,
                        rt_trips_sub.parent_station,
                        static_trips_sub.static_trip_stop_rank) AS trip_for_metrics
                WHERE
                    trip_for_metrics.first_stop_flag = FALSE
                    AND trip_for_metrics.stop_count > 1) AS t_dwell_times
            WHERE
                vehicle_events.pm_trip_id = t_dwell_times.pm_trip_id
                AND vehicle_events.service_date = t_dwell_times.service_date
                AND vehicle_events.parent_station = t_dwell_times.parent_station
                AND t_dwell_times.dwell_time_seconds IS NOT NULL
                AND t_dwell_times.dwell_time_seconds > 0
            ;
        """
        update_log = ProcessLogger(
            "dwell_time_update",
            service_date=service_date,
            static_version_key=static_version_key,
        )
        update_log.log_start()
        op.execute(stop_sync_update)
        update_log.log_complete()


def downgrade() -> None:
    pass
