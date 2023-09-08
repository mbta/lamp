"""add static directions

Revision ID: ced3caaeab23
Revises: 9d3515f1386e
Create Date: 2023-05-10 15:21:44.071212


"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ced3caaeab23"
down_revision = "9d3515f1386e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "static_directions",
        sa.Column("pk_id", sa.Integer(), nullable=False),
        sa.Column("route_id", sa.String(length=60), nullable=False),
        sa.Column("direction_id", sa.Boolean(), nullable=True),
        sa.Column("direction", sa.String(length=30), nullable=False),
        sa.Column(
            "direction_destination", sa.String(length=60), nullable=False
        ),
        sa.Column("timestamp", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("pk_id"),
    )
    op.create_index(
        op.f("ix_static_directions_direction_id"),
        "static_directions",
        ["direction_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_static_directions_route_id"),
        "static_directions",
        ["route_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_static_directions_timestamp"),
        "static_directions",
        ["timestamp"],
        unique=False,
    )

    op.add_column(
        "vehicle_trips",
        sa.Column("direction", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "vehicle_trips",
        sa.Column("direction_destination", sa.String(length=60), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("vehicle_trips", "direction_destination")
    op.drop_column("vehicle_trips", "direction")

    op.drop_index(
        op.f("ix_static_directions_timestamp"), table_name="static_directions"
    )
    op.drop_index(
        op.f("ix_static_directions_route_id"), table_name="static_directions"
    )
    op.drop_index(
        op.f("ix_static_directions_direction_id"),
        table_name="static_directions",
    )
    op.drop_table("static_directions")
