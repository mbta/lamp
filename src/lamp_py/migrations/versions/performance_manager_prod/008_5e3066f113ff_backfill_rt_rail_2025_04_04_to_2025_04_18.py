"""backfill_rt_rail_data_0404_to_0422

Revision ID: 5e3066f113ff
Revises: da8f80a3dd90
Create Date: Wed Apr 23 11:16:12 EDT 2025

Details
This will clean up missing data from RDS performance issues/outage from 4/14-4/17
This will also clean up duplication of data in prod from 4/17-4/22

This is the same as staging/012_9b461d7aa53a_backfill_rt_rail_2025_04_04_to_2025_04_22.py

* upgrade -> Delete all records from 4/4 to 4/23 in vehicle events and vehicle_trips
          -> Set all flags to "unprocessed" in metadata log from 4/4 to 4/22
* downgrade -> Nothing
"""

import os
import tempfile
import logging

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
revision = "5e3066f113ff"
down_revision = "da8f80a3dd90"
branch_labels = None
depends_on = None


def upgrade() -> None:

    # SELECT FROM vehicle_events WHERE service_date >= 20250404 AND service_date <= 20250423;"

    clear_events = "DELETE FROM vehicle_events WHERE service_date >= 20250404 AND service_date <= 20250422;"
    op.execute(clear_events)

    clear_trips = "DELETE FROM vehicle_trips WHERE service_date >= 20250404 AND service_date <= 20250422;"
    op.execute(clear_trips)

    # Query to Check
    # SELECT created_on, rail_pm_processed, rail_pm_process_fail
    # FROM public.metadata_log
    # WHERE created_on > '2025-04-04' and created_on < '2025-04-22 23:59:59'
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
            created_on > '2025-04-04 00:00:00'
            and created_on < '2025-04-22 23:59:59'
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
