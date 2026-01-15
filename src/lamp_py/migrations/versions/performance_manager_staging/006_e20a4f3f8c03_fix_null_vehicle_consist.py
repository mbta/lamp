"""fix null vehicle consist

Revision ID: e20a4f3f8c03
Revises: 96187da84955
Create Date: 2024-03-07 15:44:22.989929

On March 5th 2024, the vehicle consist field was removed from the VehiclePositions GTFS-RT feed
this broke our data pipeline requiring a switch to the multi_carriage_details field
this migration should re-process our realtime data from March 5th to present to fix missing
vehicle consist values
"""

from alembic import op
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseIndex, DatabaseManager

# revision identifiers, used by Alembic.
revision = "e20a4f3f8c03"
down_revision = "96187da84955"
branch_labels = None
depends_on = None


def upgrade() -> None:
    clear_events = "DELETE FROM vehicle_events WHERE service_date >= 20240305;"
    op.execute(clear_events)

    clear_trips = "DELETE FROM vehicle_trips WHERE service_date >= 20240305;"
    op.execute(clear_trips)

    update_md_query = r"""
        UPDATE
            metadata_log 
        SET rail_pm_processed = false
        WHERE
        (
            "path" like '%RT_VEHICLE_POSITIONS%'
            OR "path" like '%RT_TRIP_UPDATES%' 
        )
        AND
        (substring("path", 'year=(\d+)') || '-' || substring("path", 'month=(\d+)') || '-' || substring("path", 'day=(\d+)'))::date >= '2024-3-5'::date
    ;
    """
    md_manager = DatabaseManager(DatabaseIndex.METADATA)
    md_manager.execute(sa.text(update_md_query))


def downgrade() -> None:
    pass
