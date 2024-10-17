"""sync_stop_trunk

Revision ID: 2dfbde5ec151
Revises: e20a4f3f8c03
Create Date: 2024-05-09 08:52:01.675273

sync_stop_sequence values were previously based on joining generated values to 
a coalesce of branch and trunk route id's this migration will make all 
sync_stop_sequence values join to generated values based on trunk_route_id

Details
* upgrade -> iterate through service_date/static_version_key combos and update sync_stop_sequence to be based on trunk_route_id

* downgrade -> Nothing

"""

from alembic import op
import sqlalchemy as sa

from lamp_py.runtime_utils.process_logger import ProcessLogger

# revision identifiers, used by Alembic.
revision = "2dfbde5ec151"
down_revision = "e20a4f3f8c03"
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
                sync_stop_sequence = rt_sync.sync_stop_sequence
            FROM
                (
                SELECT
                    vehicle_events.pm_event_id AS pm_event_id,
                    sync_values.sync_stop_sequence AS sync_stop_sequence
                FROM
                    vehicle_events
                JOIN vehicle_trips ON
                    vehicle_events.pm_trip_id = vehicle_trips.pm_trip_id
                JOIN (
                    SELECT
                        DISTINCT static_canon.direction_id AS direction_id,
                        static_canon.trunk_route_id AS trunk_route_id,
                        static_canon.parent_station AS parent_station,
                        static_canon.static_version_key AS static_version_key,
                        ((static_canon.stop_sequence - zero_seq_vals.seq_adjust) - sync_adjust_vals.min_sync) + sync_adjust_vals.min_seq AS sync_stop_sequence
                    FROM
                        (
                        SELECT
                            canon_trips.direction_id AS direction_id,
                            canon_trips.trunk_route_id AS trunk_route_id,
                            canon_trips.route_id AS route_id,
                            static_stops.parent_station AS parent_station,
                            ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                            canon_trips.direction_id,
                            canon_trips.route_id
                        ORDER BY
                            static_stop_times.stop_sequence) AS stop_sequence,
                            canon_trips.static_version_key AS static_version_key
                        FROM
                            (
                            SELECT
                                DISTINCT ON
                                (COALESCE(static_trips.branch_route_id,
                                static_trips.trunk_route_id),
                                static_route_patterns.direction_id,
                                static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                static_route_patterns.representative_trip_id AS representative_trip_id,
                                static_trips.trunk_route_id AS trunk_route_id,
                                COALESCE(static_trips.branch_route_id,
                                static_trips.trunk_route_id) AS route_id,
                                static_route_patterns.static_version_key AS static_version_key
                            FROM
                                static_route_patterns
                            JOIN static_trips ON
                                static_route_patterns.representative_trip_id = static_trips.trip_id
                                AND static_route_patterns.static_version_key = static_trips.static_version_key
                            WHERE
                                static_route_patterns.static_version_key = {static_version_key}
                                AND (static_route_patterns.route_pattern_typicality = 1
                                    OR static_route_patterns.route_pattern_typicality = 5)
                            ORDER BY
                                COALESCE(static_trips.branch_route_id,
                                static_trips.trunk_route_id),
                                static_route_patterns.direction_id,
                                static_route_patterns.static_version_key,
                                static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                        JOIN static_stop_times ON
                            canon_trips.representative_trip_id = static_stop_times.trip_id
                                AND canon_trips.static_version_key = static_stop_times.static_version_key
                            JOIN static_stops ON
                                static_stop_times.stop_id = static_stops.stop_id
                                AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                    JOIN (
                        SELECT
                            static_canon.direction_id AS direction_id,
                            static_canon.route_id AS route_id,
                            static_canon.stop_sequence AS seq_adjust
                        FROM
                            (
                            SELECT
                                canon_trips.direction_id AS direction_id,
                                canon_trips.trunk_route_id AS trunk_route_id,
                                canon_trips.route_id AS route_id,
                                static_stops.parent_station AS parent_station,
                                ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                                canon_trips.direction_id,
                                canon_trips.route_id
                            ORDER BY
                                static_stop_times.stop_sequence) AS stop_sequence,
                                canon_trips.static_version_key AS static_version_key
                            FROM
                                (
                                SELECT
                                    DISTINCT ON
                                    (COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id),
                                    static_route_patterns.direction_id,
                                    static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                    static_route_patterns.representative_trip_id AS representative_trip_id,
                                    static_trips.trunk_route_id AS trunk_route_id,
                                    COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id) AS route_id,
                                    static_route_patterns.static_version_key AS static_version_key
                                FROM
                                    static_route_patterns
                                JOIN static_trips ON
                                    static_route_patterns.representative_trip_id = static_trips.trip_id
                                    AND static_route_patterns.static_version_key = static_trips.static_version_key
                                WHERE
                                    static_route_patterns.static_version_key = {static_version_key}
                                    AND (static_route_patterns.route_pattern_typicality = 1
                                        OR static_route_patterns.route_pattern_typicality = 5)
                                ORDER BY
                                    COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id),
                                    static_route_patterns.direction_id,
                                    static_route_patterns.static_version_key,
                                    static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                            JOIN static_stop_times ON
                                canon_trips.representative_trip_id = static_stop_times.trip_id
                                    AND canon_trips.static_version_key = static_stop_times.static_version_key
                                JOIN static_stops ON
                                    static_stop_times.stop_id = static_stops.stop_id
                                    AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                        JOIN (
                            SELECT
                                DISTINCT ON
                                (static_canon.trunk_route_id) static_canon.trunk_route_id AS trunk_route_id,
                                static_canon.parent_station AS parent_station,
                                0 AS sync_start
                            FROM
                                (
                                SELECT
                                    canon_trips.direction_id AS direction_id,
                                    canon_trips.trunk_route_id AS trunk_route_id,
                                    canon_trips.route_id AS route_id,
                                    static_stops.parent_station AS parent_station,
                                    ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                                    canon_trips.direction_id,
                                    canon_trips.route_id
                                ORDER BY
                                    static_stop_times.stop_sequence) AS stop_sequence,
                                    canon_trips.static_version_key AS static_version_key
                                FROM
                                    (
                                    SELECT
                                        DISTINCT ON
                                        (COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id),
                                        static_route_patterns.direction_id,
                                        static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                        static_route_patterns.representative_trip_id AS representative_trip_id,
                                        static_trips.trunk_route_id AS trunk_route_id,
                                        COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id) AS route_id,
                                        static_route_patterns.static_version_key AS static_version_key
                                    FROM
                                        static_route_patterns
                                    JOIN static_trips ON
                                        static_route_patterns.representative_trip_id = static_trips.trip_id
                                        AND static_route_patterns.static_version_key = static_trips.static_version_key
                                    WHERE
                                        static_route_patterns.static_version_key = {static_version_key}
                                        AND (static_route_patterns.route_pattern_typicality = 1
                                            OR static_route_patterns.route_pattern_typicality = 5)
                                    ORDER BY
                                        COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id),
                                        static_route_patterns.direction_id,
                                        static_route_patterns.static_version_key,
                                        static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                                JOIN static_stop_times ON
                                    canon_trips.representative_trip_id = static_stop_times.trip_id
                                        AND canon_trips.static_version_key = static_stop_times.static_version_key
                                    JOIN static_stops ON
                                        static_stop_times.stop_id = static_stops.stop_id
                                        AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                            GROUP BY
                                static_canon.trunk_route_id,
                                static_canon.parent_station
                            ORDER BY
                                static_canon.trunk_route_id,
                                count(static_canon.stop_sequence) DESC,
                                max(static_canon.stop_sequence) - min(static_canon.stop_sequence) DESC) AS zero_points ON
                            zero_points.trunk_route_id = static_canon.trunk_route_id
                                AND zero_points.parent_station = static_canon.parent_station) AS zero_seq_vals ON
                        zero_seq_vals.direction_id = static_canon.direction_id
                        AND zero_seq_vals.route_id = static_canon.route_id
                    JOIN (
                        SELECT
                            static_canon.direction_id AS direction_id,
                            static_canon.trunk_route_id AS trunk_route_id,
                            min(static_canon.stop_sequence) AS min_seq,
                            min(static_canon.stop_sequence - zero_seq_vals.seq_adjust) AS min_sync
                        FROM
                            (
                            SELECT
                                canon_trips.direction_id AS direction_id,
                                canon_trips.trunk_route_id AS trunk_route_id,
                                canon_trips.route_id AS route_id,
                                static_stops.parent_station AS parent_station,
                                ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                                canon_trips.direction_id,
                                canon_trips.route_id
                            ORDER BY
                                static_stop_times.stop_sequence) AS stop_sequence,
                                canon_trips.static_version_key AS static_version_key
                            FROM
                                (
                                SELECT
                                    DISTINCT ON
                                    (COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id),
                                    static_route_patterns.direction_id,
                                    static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                    static_route_patterns.representative_trip_id AS representative_trip_id,
                                    static_trips.trunk_route_id AS trunk_route_id,
                                    COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id) AS route_id,
                                    static_route_patterns.static_version_key AS static_version_key
                                FROM
                                    static_route_patterns
                                JOIN static_trips ON
                                    static_route_patterns.representative_trip_id = static_trips.trip_id
                                    AND static_route_patterns.static_version_key = static_trips.static_version_key
                                WHERE
                                    static_route_patterns.static_version_key = {static_version_key}
                                    AND (static_route_patterns.route_pattern_typicality = 1
                                        OR static_route_patterns.route_pattern_typicality = 5)
                                ORDER BY
                                    COALESCE(static_trips.branch_route_id,
                                    static_trips.trunk_route_id),
                                    static_route_patterns.direction_id,
                                    static_route_patterns.static_version_key,
                                    static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                            JOIN static_stop_times ON
                                canon_trips.representative_trip_id = static_stop_times.trip_id
                                    AND canon_trips.static_version_key = static_stop_times.static_version_key
                                JOIN static_stops ON
                                    static_stop_times.stop_id = static_stops.stop_id
                                    AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                        JOIN (
                            SELECT
                                static_canon.direction_id AS direction_id,
                                static_canon.route_id AS route_id,
                                static_canon.stop_sequence AS seq_adjust
                            FROM
                                (
                                SELECT
                                    canon_trips.direction_id AS direction_id,
                                    canon_trips.trunk_route_id AS trunk_route_id,
                                    canon_trips.route_id AS route_id,
                                    static_stops.parent_station AS parent_station,
                                    ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                                    canon_trips.direction_id,
                                    canon_trips.route_id
                                ORDER BY
                                    static_stop_times.stop_sequence) AS stop_sequence,
                                    canon_trips.static_version_key AS static_version_key
                                FROM
                                    (
                                    SELECT
                                        DISTINCT ON
                                        (COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id),
                                        static_route_patterns.direction_id,
                                        static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                        static_route_patterns.representative_trip_id AS representative_trip_id,
                                        static_trips.trunk_route_id AS trunk_route_id,
                                        COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id) AS route_id,
                                        static_route_patterns.static_version_key AS static_version_key
                                    FROM
                                        static_route_patterns
                                    JOIN static_trips ON
                                        static_route_patterns.representative_trip_id = static_trips.trip_id
                                        AND static_route_patterns.static_version_key = static_trips.static_version_key
                                    WHERE
                                        static_route_patterns.static_version_key = {static_version_key}
                                        AND (static_route_patterns.route_pattern_typicality = 1
                                            OR static_route_patterns.route_pattern_typicality = 5)
                                    ORDER BY
                                        COALESCE(static_trips.branch_route_id,
                                        static_trips.trunk_route_id),
                                        static_route_patterns.direction_id,
                                        static_route_patterns.static_version_key,
                                        static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                                JOIN static_stop_times ON
                                    canon_trips.representative_trip_id = static_stop_times.trip_id
                                        AND canon_trips.static_version_key = static_stop_times.static_version_key
                                    JOIN static_stops ON
                                        static_stop_times.stop_id = static_stops.stop_id
                                        AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                            JOIN (
                                SELECT
                                    DISTINCT ON
                                    (static_canon.trunk_route_id) static_canon.trunk_route_id AS trunk_route_id,
                                    static_canon.parent_station AS parent_station,
                                    0 AS sync_start
                                FROM
                                    (
                                    SELECT
                                        canon_trips.direction_id AS direction_id,
                                        canon_trips.trunk_route_id AS trunk_route_id,
                                        canon_trips.route_id AS route_id,
                                        static_stops.parent_station AS parent_station,
                                        ROW_NUMBER() OVER (PARTITION BY canon_trips.static_version_key,
                                        canon_trips.direction_id,
                                        canon_trips.route_id
                                    ORDER BY
                                        static_stop_times.stop_sequence) AS stop_sequence,
                                        canon_trips.static_version_key AS static_version_key
                                    FROM
                                        (
                                        SELECT
                                            DISTINCT ON
                                            (COALESCE(static_trips.branch_route_id,
                                            static_trips.trunk_route_id),
                                            static_route_patterns.direction_id,
                                            static_route_patterns.static_version_key) static_route_patterns.direction_id AS direction_id,
                                            static_route_patterns.representative_trip_id AS representative_trip_id,
                                            static_trips.trunk_route_id AS trunk_route_id,
                                            COALESCE(static_trips.branch_route_id,
                                            static_trips.trunk_route_id) AS route_id,
                                            static_route_patterns.static_version_key AS static_version_key
                                        FROM
                                            static_route_patterns
                                        JOIN static_trips ON
                                            static_route_patterns.representative_trip_id = static_trips.trip_id
                                            AND static_route_patterns.static_version_key = static_trips.static_version_key
                                        WHERE
                                            static_route_patterns.static_version_key = {static_version_key}
                                            AND (static_route_patterns.route_pattern_typicality = 1
                                                OR static_route_patterns.route_pattern_typicality = 5)
                                        ORDER BY
                                            COALESCE(static_trips.branch_route_id,
                                            static_trips.trunk_route_id),
                                            static_route_patterns.direction_id,
                                            static_route_patterns.static_version_key,
                                            static_route_patterns.route_pattern_typicality DESC) AS canon_trips
                                    JOIN static_stop_times ON
                                        canon_trips.representative_trip_id = static_stop_times.trip_id
                                            AND canon_trips.static_version_key = static_stop_times.static_version_key
                                        JOIN static_stops ON
                                            static_stop_times.stop_id = static_stops.stop_id
                                            AND static_stop_times.static_version_key = static_stops.static_version_key) AS static_canon
                                GROUP BY
                                    static_canon.trunk_route_id,
                                    static_canon.parent_station
                                ORDER BY
                                    static_canon.trunk_route_id,
                                    count(static_canon.stop_sequence) DESC,
                                    max(static_canon.stop_sequence) - min(static_canon.stop_sequence) DESC) AS zero_points ON
                                zero_points.trunk_route_id = static_canon.trunk_route_id
                                    AND zero_points.parent_station = static_canon.parent_station) AS zero_seq_vals ON
                            zero_seq_vals.direction_id = static_canon.direction_id
                            AND zero_seq_vals.route_id = static_canon.route_id
                        GROUP BY
                            static_canon.direction_id,
                            static_canon.trunk_route_id) AS sync_adjust_vals ON
                        sync_adjust_vals.direction_id = static_canon.direction_id
                        AND sync_adjust_vals.trunk_route_id = static_canon.trunk_route_id) AS sync_values ON
                    vehicle_trips.direction_id = sync_values.direction_id
                    AND vehicle_trips.trunk_route_id = sync_values.trunk_route_id
                    AND vehicle_trips.static_version_key = sync_values.static_version_key
                    AND vehicle_events.parent_station = sync_values.parent_station
                WHERE
                    vehicle_events.service_date = {service_date}) AS rt_sync
            WHERE
                vehicle_events.pm_event_id = rt_sync.pm_event_id
            ;
        """
        update_log = ProcessLogger(
            "sync_stop_sequence_to_trunk_id",
            service_date=service_date,
            static_version_key=static_version_key,
        )
        update_log.log_start()
        op.execute(stop_sync_update)
        update_log.log_complete()


def downgrade() -> None:
    pass
