"""create opmi all fields view

Revision ID: 171891fde1cf
Revises: 4fe83fd4091d
Create Date: 2023-05-30 13:15:31.631938

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '171891fde1cf'
down_revision = '4fe83fd4091d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    update_view = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            ve.pk_id as vehicle_events_pk_id
            , ve.trip_hash
            , ve.trip_stop_hash
            , ve.stop_sequence
            , ve.stop_id
            , ve.parent_station
            , ve.vp_move_timestamp
            , ve.vp_stop_timestamp
            , ve.tu_stop_timestamp
            , vt.direction_id
            , vt.route_id
            , vt.branch_route_id
            , vt.trunk_route_id
            , vt.service_date
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
    op.execute(update_view)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS opmi_all_rt_fields_joined;")
