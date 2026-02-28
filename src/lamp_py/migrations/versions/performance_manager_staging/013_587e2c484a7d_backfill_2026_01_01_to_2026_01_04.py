"""Backfill 2026-01-01 to 2026-01-04

Revision ID: 587e2c484a7d
Revises: 9b461d7aa53a
Create Date: 2026-01-16 13:35:25.642349

* upgrade   -> Set all flags to unprocessed in metadata log from 2025-01-01 to 2026-01-04 in vehicle_events and vehicle_trips.
               Delete records in vehicle_events and vehicle_trips.
               This *should* result in automatic updates to vehicle_events and vehicle_trips.
* downgrade -> nothing
"""

import logging

from alembic import op
import sqlalchemy as sa

from lamp_py.postgres.postgres_utils import DatabaseIndex, DatabaseManager


# revision identifiers, used by Alembic.
revision = "587e2c484a7d"
down_revision = "9b461d7aa53a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    clear_events = "DELETE FROM vehicle_events WHERE service_date BETWEEN 20260101 AND 20260104;"
    op.execute(clear_events)

    clear_trips = "DELETE FROM vehicle_trips WHERE service_date BETWEEN 20260101 AND 20260104;"
    op.execute(clear_trips)

    try:
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
        md_manager = DatabaseManager(DatabaseIndex.METADATA)
        md_manager.execute(sa.text(update_md_query))

    except sa.exc.ProgrammingError as error:
        # Error 42P01 is an 'Undefined Table' error. This occurs when there is
        # no metadata_log table in the rail performance manager database
        #
        # Raise all other sql errors
        original_error = error.orig
        if original_error is not None and hasattr(original_error, "pgcode") and original_error.pgcode == "42P01":
            logging.info("No Metadata Table in Rail Performance Manager")
        else:
            raise


def downgrade() -> None:
    pass
