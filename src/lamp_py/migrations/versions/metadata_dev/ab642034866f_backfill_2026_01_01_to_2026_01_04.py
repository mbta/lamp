"""Backfill 2026-01-01 to 2026-01-04

Revision ID: ab642034866f
Revises: 26db393ea854
Create Date: 2026-01-15 14:08:44.338406

Backfill missing performance data from 2026-01-01 to 2026-01-04.

* upgrade   -> Set all flags to unprocessed in metadata log from 2025-01-01 to 2026-01-04 in vehicle_events and vehicle_trips.
               This *should* result in automatic updates to vehicle_events and trip_updates
* downgrade -> nothing
"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'ab642034866f'
down_revision = '26db393ea854'
branch_labels = None
depends_on = None


def upgrade() -> None:
    update_md_query = """
        UPDATE
            metadata_log
        SET
            rail_pm_process_fail = false
            , rail_pm_processed = false
        WHERE
            substring(path, '\d{4}-\d{2}-\d{2}')::date >= '2026-01-01'
            and substring(path, '\d{4}-\d{2}-\d{2}')::date <= '2026-01-04'
            and (
                path LIKE '%/RT_TRIP_UPDATES/%'
                or path LIKE '%/RT_VEHICLE_POSITIONS/%'
            )
        ;
        """
    op.execute(update_md_query)


def downgrade() -> None:
    pass
