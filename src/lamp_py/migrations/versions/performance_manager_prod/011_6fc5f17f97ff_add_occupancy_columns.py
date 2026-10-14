"""add occupancy columns

Revision ID: 6fc5f17f97ff
Revises: 7cb3dbb1dac0
Create Date: 2026-09-09 18:12:44.913287

Adds nullable occupancy_status and occupancy_percentage columns holding pipe
delimited per-carriage values from GTFS-RT multi_carriage_details. Existing
rows stay NULL unless RT_VEHICLE_POSITIONS are re-processed.

Details
* upgrade -> add occupancy columns to vehicle_events and temp_event_compare

* downgrade -> drop occupancy columns from both tables

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "6fc5f17f97ff"
down_revision = "7cb3dbb1dac0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vehicle_events", sa.Column("occupancy_status", sa.String(), nullable=True))
    op.add_column("vehicle_events", sa.Column("occupancy_percentage", sa.String(), nullable=True))
    op.add_column("temp_event_compare", sa.Column("occupancy_status", sa.String(), nullable=True))
    op.add_column("temp_event_compare", sa.Column("occupancy_percentage", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("temp_event_compare", "occupancy_percentage")
    op.drop_column("temp_event_compare", "occupancy_status")
    op.drop_column("vehicle_events", "occupancy_percentage")
    op.drop_column("vehicle_events", "occupancy_status")
