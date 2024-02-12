"""extend service_id_by_date_and_route

Revision ID: ae6c6e4b2df5
Revises: 1b53fd278b10
Create Date: 2023-12-17 06:56:17.330783

Details
* upgrade -> extend service_id_by_date_and_route VIEW to generate values past current date
* upgrade -> update canonical_stop_sequence to use row_number function instead of direct from static schedule

* downgrade -> Nothing
"""

from alembic import op

from lamp_py.migrations.versions.performance_manager_staging.sql_strings.strings_003 import (
    view_service_id_by_date_and_route,
)


# revision identifiers, used by Alembic.
revision = "ae6c6e4b2df5"
down_revision = "1b53fd278b10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS service_id_by_date_and_route;")
    op.execute(view_service_id_by_date_and_route)

    op.create_index(
        "ix_static_trips_composite_4",
        "static_trips",
        ["static_version_key", "service_id"],
        unique=False,
    )

    update_stop_sequences = (
        "UPDATE vehicle_events "
        "SET canonical_stop_sequence = static_canon.stop_sequence "
        "FROM vehicle_events AS ve "
        "JOIN vehicle_trips AS vt "
        "ON ve.pm_trip_id = vt.pm_trip_id "
        "JOIN "
        "("
        "    select "
        "      srp.direction_id "
        "    , coalesce(st.branch_route_id, st.trunk_route_id) AS route_id "
        "    , ROW_NUMBER () OVER (PARTITION BY srp.static_version_key, srp.direction_id, coalesce(st.branch_route_id, st.trunk_route_id) ORDER BY sst.stop_sequence) AS stop_sequence"
        "    , ss.parent_station "
        "    , srp.static_version_key "
        "    from static_route_patterns srp "
        "    JOIN static_trips st "
        "    ON srp.representative_trip_id = st.trip_id "
        "    AND srp.static_version_key = st.static_version_key "
        "    JOIN static_stop_times sst "
        "    ON srp.representative_trip_id = sst.trip_id "
        "    AND srp.static_version_key = sst.static_version_key "
        "    JOIN static_stops ss "
        "    ON sst.stop_id = ss.stop_id "
        "    AND sst.static_version_key = ss.static_version_key "
        "    WHERE "
        "    srp.route_pattern_typicality = 1"
        ") AS static_canon "
        "ON ve.parent_station = static_canon.parent_station "
        "AND vt.static_version_key = static_canon.static_version_key "
        "AND vt.direction_id = static_canon.direction_id "
        "AND coalesce(vt.branch_route_id, vt.trunk_route_id) = static_canon.route_id "
        "WHERE vehicle_events.pm_trip_id = ve.pm_trip_id "
        "AND vehicle_events.parent_station = static_canon.parent_station "
        ";"
    )
    op.execute(update_stop_sequences)


def downgrade() -> None:
    op.drop_index("ix_static_trips_composite_4", table_name="static_trips")
