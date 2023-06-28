"""add prev and next trip stops

Revision ID: 37d97f420d54
Revises: 171891fde1cf
Create Date: 2023-06-22 13:23:24.497833

Details:
* upgrade -> adds `previous_trip_stop_pk_id` column and index to `vehicle_events` table
* upgrade -> adds `next_trip_stop_pk_id` column and index to `vehicle_events` table
* upgrade -> revises `opmi_all_rt_fields_joined` VIEW to use new trip_stop_pk_id columns
* upgrade -> runs update command to populate `previous_trip_stop_pk_id` and `next_trip_stop_pk_id` in table with existing records

* downgrade -> drops `previous_trip_stop_pk_id` column and index from `vehicle_events` table
* downgrade -> drops `next_trip_stop_pk_id` column and index from `vehicle_events` table
* downgrade -> rollback `opmi_all_rt_fields_joined` VIEW to use windows functions

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "37d97f420d54"
down_revision = "171891fde1cf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "vehicle_events",
        sa.Column("previous_trip_stop_pk_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "vehicle_events",
        sa.Column("next_trip_stop_pk_id", sa.Integer(), nullable=True),
    )
    update_columns = """
        WITH prev_next_trip_stops AS (
            SELECT 
                vehicle_events.pk_id AS pk_id
                ,lead(vehicle_events.pk_id) OVER (PARTITION BY vehicle_events.trip_hash ORDER BY vehicle_events.stop_sequence) AS next_trip_stop_pk_id
                ,lag(vehicle_events.pk_id) OVER (PARTITION BY vehicle_events.trip_hash ORDER BY vehicle_events.stop_sequence) AS previous_trip_stop_pk_id 
            FROM 
                vehicle_events 
        )
        UPDATE
            vehicle_events 
        SET 
            previous_trip_stop_pk_id=prev_next_trip_stops.previous_trip_stop_pk_id
            ,next_trip_stop_pk_id=prev_next_trip_stops.next_trip_stop_pk_id 
        FROM 
            prev_next_trip_stops 
        WHERE 
            vehicle_events.pk_id = prev_next_trip_stops.pk_id
        ;
    """
    op.execute(update_columns)

    op.create_index(
        op.f("ix_vehicle_events_next_trip_stop_pk_id"),
        "vehicle_events",
        ["next_trip_stop_pk_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_vehicle_events_previous_trip_stop_pk_id"),
        "vehicle_events",
        ["previous_trip_stop_pk_id"],
        unique=False,
    )

    update_view = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            vt.service_date
            , ve.trip_hash
            , ve.trip_stop_hash
            , ve.stop_sequence
            , ve.stop_id
            , prev_ve.stop_id as previous_stop_id
            , ve.parent_station
            , prev_ve.parent_station as previous_parent_station
            , ve.vp_move_timestamp
            , COALESCE(ve.vp_stop_timestamp, ve.tu_stop_timestamp) as vp_tu_stop_timestamp
            , vt.direction_id
            , vt.route_id
            , vt.branch_route_id
            , vt.trunk_route_id
            , vt.start_time
            , vt.vehicle_id
            , vt.stop_count
            , vt.trip_id
            , vt.vehicle_label
            , vt.vehicle_consist
            , vt.direction
            , vt.direction_destination
            , vt.static_trip_id_guess
            , vt.static_start_time
            , vt.static_stop_count
            , vt.first_last_station_match
            , vt.static_version_key
            , vem.travel_time_seconds
            , vem.dwell_time_seconds
            , vem.headway_trunk_seconds
            , vem.headway_branch_seconds
            , COALESCE(vem.updated_on, ve.updated_on) as updated_on
        FROM 
            vehicle_events ve
        LEFT JOIN
            vehicle_trips vt
        ON 
            ve.trip_hash = vt.trip_hash
        LEFT JOIN
            vehicle_events prev_ve
        ON
            ve.pk_id = prev_ve.previous_trip_stop_pk_id
        LEFT JOIN 
            vehicle_event_metrics vem
        ON
            ve.trip_stop_hash = vem.trip_stop_hash
        ;
    """
    op.execute("DROP VIEW IF EXISTS opmi_all_rt_fields_joined;")
    op.execute(update_view)


def downgrade() -> None:
    update_view = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            vt.service_date
            , ve.trip_hash
            , ve.trip_stop_hash
            , ve.stop_sequence
            , ve.stop_id
            , LAG (ve.stop_id, 1) OVER (PARTITION BY ve.trip_hash ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_stop_id
            , ve.parent_station
            , LAG (ve.parent_station, 1) OVER (PARTITION BY ve.trip_hash ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_parent_station
            , ve.vp_move_timestamp
            , COALESCE(ve.vp_stop_timestamp, ve.tu_stop_timestamp) as vp_tu_stop_timestamp
            , vt.direction_id
            , vt.route_id
            , vt.branch_route_id
            , vt.trunk_route_id
            , vt.start_time
            , vt.vehicle_id
            , vt.stop_count
            , vt.trip_id
            , vt.vehicle_label
            , vt.vehicle_consist
            , vt.direction
            , vt.direction_destination
            , vt.static_trip_id_guess
            , vt.static_start_time
            , vt.static_stop_count
            , vt.first_last_station_match
            , vt.static_version_key
            , vem.travel_time_seconds
            , vem.dwell_time_seconds
            , vem.headway_trunk_seconds
            , vem.headway_branch_seconds
            , COALESCE(vem.updated_on, ve.updated_on) as updated_on
        FROM 
            vehicle_events ve
        LEFT JOIN
            vehicle_trips vt
        ON 
            ve.trip_hash = vt.trip_hash
        LEFT JOIN 
            vehicle_event_metrics vem
        ON
            ve.trip_stop_hash = vem.trip_stop_hash
        ;
    """
    op.execute("DROP VIEW IF EXISTS opmi_all_rt_fields_joined;")
    op.execute(update_view)

    op.drop_index(
        op.f("ix_vehicle_events_previous_trip_stop_pk_id"),
        table_name="vehicle_events",
    )
    op.drop_index(
        op.f("ix_vehicle_events_next_trip_stop_pk_id"),
        table_name="vehicle_events",
    )
    op.drop_column("vehicle_events", "next_trip_stop_pk_id")
    op.drop_column("vehicle_events", "previous_trip_stop_pk_id")
