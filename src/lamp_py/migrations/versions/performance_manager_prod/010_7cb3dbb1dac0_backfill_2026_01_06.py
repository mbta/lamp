"""backfill_2026_01_06

    Revision ID: 7cb3dbb1dac0
    Revises: f138635d1338
    Create Date: 2026-03-01 09:05:04.036779

    Details: Query of metadata table revealed that 1/6 also failed processing, so generating that again

    * upgrade -> same as previous migration but for 1/6
    * downgrade -> None
    """

import logging
import os
import tempfile
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List

from alembic import op
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError

from lamp_py.aws.s3 import download_file, upload_file
from lamp_py.postgres.postgres_utils import DatabaseIndex, DatabaseManager

# revision identifiers, used by Alembic.
revision = "7cb3dbb1dac0"
down_revision = "f138635d1338"
branch_labels = None # tbd
depends_on = None #tbd


def upgrade() -> None:

    # SELECT FROM vehicle_events WHERE service_date >= 20250404 AND service_date <= 20250423;"

    clear_events = "DELETE FROM vehicle_events WHERE service_date = 20260106;"
    op.execute(clear_events)

    clear_trips = "DELETE FROM vehicle_trips WHERE service_date = 20260106;"
    op.execute(clear_trips)

    # Query to Check
    # SELECT created_on, rail_pm_processed, rail_pm_process_fail
    # FROM public.metadata_log
    # WHERE created_on > '2026-01-06' and created_on < '2026-01-06 23:59:59'
    # AND (path LIKE '%/RT_TRIP_UPDATES/%' or path LIKE '%/RT_VEHICLE_POSITIONS/%')
    # ORDER BY created_on;

    try:
        update_md_query = """
        UPDATE
            metadata_log
        SET
            rail_pm_process_fail = false
            , rail_pm_processed = false
        WHERE
            created_on > '2026-01-06 00:00:00'
            and created_on < '2026-01-06 23:59:59'
            and (
                path LIKE '%/RT_TRIP_UPDATES/%'
                or path LIKE '%/RT_VEHICLE_POSITIONS/%'
            )
        ;
        """
        md_manager = DatabaseManager(DatabaseIndex.METADATA)
        md_manager.execute(sa.text(update_md_query))

    except ProgrammingError as error:
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
    