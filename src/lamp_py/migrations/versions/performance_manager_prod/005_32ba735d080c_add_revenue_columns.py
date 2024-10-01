"""add revenue columns

Revision ID: 32ba735d080c
Revises: 896dedd8a4db
Create Date: 2024-09-20 08:47:52.784591

This change adds a boolean revenue column to the vehcile_trips table.
Initially this will be filled with True and back-filled by a seperate operation

Details
* upgrade -> drop triggers and indexes from table and add revenue column

* downgrade -> drop revenue column

"""

from alembic import op
import sqlalchemy as sa

from lamp_py.postgres.rail_performance_manager_schema import (
    TempEventCompare,
    VehicleTrips,
)

# revision identifiers, used by Alembic.
revision = "32ba735d080c"
down_revision = "896dedd8a4db"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        f"ALTER TABLE public.vehicle_trips DISABLE TRIGGER rt_trips_update_branch_trunk;"
    )
    op.execute(
        f"ALTER TABLE public.vehicle_trips DISABLE TRIGGER update_vehicle_trips_modified;"
    )
    op.drop_index("ix_vehicle_trips_composite_1", table_name="vehicle_trips")
    op.drop_constraint("vehicle_trips_unique_trip", table_name="vehicle_trips")

    op.add_column(
        "temp_event_compare", sa.Column("revenue", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "vehicle_trips", sa.Column("revenue", sa.Boolean(), nullable=True)
    )
    op.execute(sa.update(TempEventCompare).values(revenue=True))
    op.execute(sa.update(VehicleTrips).values(revenue=True))
    op.alter_column("temp_event_compare", "revenue", nullable=False)
    op.alter_column("vehicle_trips", "revenue", nullable=False)

    op.create_unique_constraint(
        "vehicle_trips_unique_trip",
        "vehicle_trips",
        ["service_date", "route_id", "trip_id"],
    )
    op.create_index(
        "ix_vehicle_trips_composite_1",
        "vehicle_trips",
        ["route_id", "direction_id", "vehicle_id"],
        unique=False,
    )
    op.execute(
        f"ALTER TABLE public.vehicle_trips ENABLE TRIGGER rt_trips_update_branch_trunk;"
    )
    op.execute(
        f"ALTER TABLE public.vehicle_trips ENABLE TRIGGER update_vehicle_trips_modified;"
    )


def downgrade() -> None:
    op.drop_column("vehicle_trips", "revenue")
    op.drop_column("temp_event_compare", "revenue")
