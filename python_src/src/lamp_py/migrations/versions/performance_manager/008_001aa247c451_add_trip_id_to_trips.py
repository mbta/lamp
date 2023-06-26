"""add trip_id to trips

Revision ID: 001aa247c451
Revises: ed73250b7656
Create Date: 2023-05-01 16:12:21.591318

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "001aa247c451"
down_revision = "ed73250b7656"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "vehicle_trips",
        sa.Column("trip_id", sa.String(length=128), nullable=True),
    )
    op.alter_column(
        "temp_hash_compare",
        "trip_stop_hash",
        new_column_name="hash",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("vehicle_trips", "trip_id")
    op.alter_column(
        "temp_hash_compare",
        "hash",
        new_column_name="trip_stop_hash",
    )
    # ### end Alembic commands ###