"""remove trip fields from events

Revision ID: 18d962b3df59
Revises: 97f5fb821b8a
Create Date: 2023-05-17 13:06:46.832362

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "18d962b3df59"
down_revision = "97f5fb821b8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("vehicle_events", "service_date")
    op.drop_column("vehicle_events", "route_id")
    op.drop_column("vehicle_events", "vehicle_id")
    op.drop_column("vehicle_events", "start_time")
    op.drop_column("vehicle_events", "direction_id")


def downgrade() -> None:
    op.add_column(
        "vehicle_events",
        sa.Column(
            "direction_id", sa.BOOLEAN(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "vehicle_events",
        sa.Column(
            "start_time", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
    op.add_column(
        "vehicle_events",
        sa.Column(
            "vehicle_id",
            sa.VARCHAR(length=60),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "vehicle_events",
        sa.Column(
            "route_id",
            sa.VARCHAR(length=60),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.add_column(
        "vehicle_events",
        sa.Column(
            "service_date", sa.INTEGER(), autoincrement=False, nullable=True
        ),
    )
