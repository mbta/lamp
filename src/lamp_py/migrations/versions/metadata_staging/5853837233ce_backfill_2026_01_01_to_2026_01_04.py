"""Backfill 2026-01-01 to 2026-01-04

Revision ID: 5853837233ce
Revises: a08c5fd37dbd
Create Date: 2026-01-15 15:57:38.243775

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "5853837233ce"
down_revision = "a08c5fd37dbd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    update_md_query = r"""
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
