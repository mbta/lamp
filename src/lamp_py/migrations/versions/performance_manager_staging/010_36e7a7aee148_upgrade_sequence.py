"""upgrade sequence

Revision ID: 36e7a7aee148
Revises: 32ba735d080c
Create Date: 2025-01-07 13:57:50.433896

This change upgrades the pm_event_id sequence type to bigint to avoid running out of keys

Details
* upgrade -> drop opmi view, upgrade sequence, update sequence storage columns

* downgrade -> not possible, can't go from bigint to int

"""

from alembic import op
import sqlalchemy as sa

from lamp_py.migrations.versions.performance_manager_staging.sql_strings.strings_001 import view_opmi_all_rt_fields_joined

# revision identifiers, used by Alembic.
revision = "36e7a7aee148"
down_revision = "32ba735d080c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Upgrade sequence to BIGINT
    op.execute("ALTER SEQUENCE vehicle_events_pm_event_id_seq as bigint MAXVALUE 9223372036854775807;")
    # DROP VIEW before upgrading columns
    drop_opmi_all_rt_fields_joined = "DROP VIEW IF EXISTS opmi_all_rt_fields_joined;"
    op.execute(drop_opmi_all_rt_fields_joined)
    # Upgrade event_id columns to BIGINT
    op.alter_column(
        "vehicle_events",
        "pm_event_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "vehicle_events",
        "previous_trip_stop_pm_event_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.alter_column(
        "vehicle_events",
        "next_trip_stop_pm_event_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.execute(view_opmi_all_rt_fields_joined)


def downgrade() -> None:
    # Can not migrate from INT to BIGINT without losing data.
    pass
