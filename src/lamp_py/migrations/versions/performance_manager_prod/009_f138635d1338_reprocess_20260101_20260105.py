"""regen-rail-perf-20260101-to-20260104

    Revision ID: f138635d1338
    Revises: 5e3066f113ff
    Create Date: 2026-02-28 11:31:59.737856

    Details: regen-rail-perf-20260101-to-20260104 - 
    This will clean up missing data from RDS performance issues/outage from 1/1 to 1/4
    Rerunning 1/1 - 1/5 just in case. 
    * upgrade -> Delete all records from 1/1 to 1/5 in vehicle events and vehicle_trips
              -> Set all flags to "unprocessed" in metadata log from 1/1 to 1/5
    * downgrade -> Nothing
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
revision = "f138635d1338"
down_revision = "5e3066f113ff"
branch_labels = None # tbd
depends_on = None #tbd


def upgrade() -> None:

    # SELECT FROM vehicle_events WHERE service_date >= 20250404 AND service_date <= 20250423;"

    clear_events = "DELETE FROM vehicle_events WHERE service_date >= 20260101 AND service_date <= 20260105;"
    op.execute(clear_events)

    clear_trips = "DELETE FROM vehicle_trips WHERE service_date >= 20260101 AND service_date <= 20260105;"
    op.execute(clear_trips)

    # Query to Check
    # SELECT created_on, rail_pm_processed, rail_pm_process_fail
    # FROM public.metadata_log
    # WHERE created_on > '2026-01-01' and created_on < '2026-01-05 23:59:59'
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
            created_on > '2026-01-01 00:00:00'
            and created_on < '2026-01-05 23:59:59'
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
