"""migrate canonical_stop_sequence values

Revision ID: 45dedc21086e
Revises: ae6c6e4b2df5
Create Date: 2023-12-21 14:40:07.605800

Details
* upgrade -> update canonical_stop_sequence values to use route_pattern_typicality=5 and then 1, if 5 does not exist
this condition was introduced by 3 schedules in Dec 2023:
- Fall 2023, 2023-12-07T22:35:17+00:00, version D
- Fall 2023, 2023-12-05T18:45:28+00:00, version D
- Fall 2023, 2023-12-04T21:09:26+00:00, version D

* downgrade -> Nothing
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "45dedc21086e"
down_revision = "ae6c6e4b2df5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    update_canon_stop = """
    UPDATE
        vehicle_events
    SET
        canonical_stop_sequence = static_canon.stop_sequence
    FROM
        vehicle_events AS ve
    JOIN 
        vehicle_trips AS vt 
    ON 
        ve.pm_trip_id = vt.pm_trip_id
    JOIN 
    (
        SELECT
            canon_trips.direction_id,
            canon_trips.route_id,
            static_stops.parent_station,
            row_number() OVER (
                PARTITION BY canon_trips.static_version_key,
                canon_trips.direction_id,
                canon_trips.route_id
                ORDER BY
                    static_stop_times.stop_sequence
            ) AS stop_sequence,
            canon_trips.static_version_key
        FROM
        (
            SELECT
                DISTINCT ON (
                    coalesce(static_trips.branch_route_id, static_trips.trunk_route_id),
                    static_route_patterns.direction_id,
                    static_route_patterns.static_version_key
                ) 
                static_route_patterns.direction_id AS direction_id,
                static_route_patterns.representative_trip_id AS representative_trip_id,
                coalesce(static_trips.branch_route_id, static_trips.trunk_route_id) AS route_id,
                static_route_patterns.static_version_key AS static_version_key
            FROM
                static_route_patterns
            JOIN 
                static_trips 
            ON 
                static_route_patterns.representative_trip_id = static_trips.trip_id
                AND static_route_patterns.static_version_key = static_trips.static_version_key
            WHERE
                static_route_patterns.route_pattern_typicality = 1
                OR static_route_patterns.route_pattern_typicality = 5
            ORDER BY
                coalesce(static_trips.branch_route_id, static_trips.trunk_route_id),
                static_route_patterns.direction_id,
                static_route_patterns.static_version_key,
                static_route_patterns.route_pattern_typicality DESC
        ) AS canon_trips
        JOIN 
            static_stop_times 
        ON 
            canon_trips.representative_trip_id = static_stop_times.trip_id
            AND canon_trips.static_version_key = static_stop_times.static_version_key
        JOIN 
            static_stops 
        ON 
            static_stop_times.stop_id = static_stops.stop_id
            AND static_stop_times.static_version_key = static_stops.static_version_key
    ) AS static_canon 
    ON 
        ve.parent_station = static_canon.parent_station
        AND vt.static_version_key = static_canon.static_version_key
        AND vt.direction_id = static_canon.direction_id
        AND coalesce(vt.branch_route_id, vt.trunk_route_id) = static_canon.route_id
    WHERE
        vehicle_events.pm_trip_id = ve.pm_trip_id
        AND vehicle_events.parent_station = static_canon.parent_station
    ;
    """
    op.execute(update_canon_stop)


def downgrade() -> None:
    pass
