"""Backfilling 2026-01-01 to 2026-01-04

Revision ID: f826082c2579
Revises: 36e7a7aee148
Create Date: 2026-01-15 10:50:09.148207

Backfill missing performance data from 2026-01-01 to 2026-01-04.

* upgrade   -> Set all flags to unprocessed in metadata log from 2025-01-01 to 2026-01-04 in vehicle_events and vehicle_trips.
               This *should* result in automatic updates to vehicle_events and trip_updates
* downgrade -> nothing
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "f826082c2579"
down_revision = "36e7a7aee148"
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
