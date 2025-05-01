"""backfill_rt_rail_data_0404_to_0422

    Revision ID: a08c5fd37dbd
    Revises: 26db393ea854
    Create Date: 2025-05-01 00:00:00

    Details: Reprocess 4/22 because it is missing. Include 4/22 and 4/23 because of UTC vs EST

    * upgrade -> reset processed flags in metadata for 4/22 and 4/23
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
revision = "a08c5fd37dbd"
down_revision = "26db393ea854"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass

    #     lamp_metadata=>     SELECT path, created_on, rail_pm_processed, rail_pm_process_fail
    #     FROM public.metadata_log
    #     WHERE substring(path, '\d{4}-\d{2}-\d{2}')::date >= '2025-04-22'
    #     and substring(path, '\d{4}-\d{2}-\d{2}')::date <= '2025-04-23'
    #     and (
    #         path LIKE '%/RT_TRIP_UPDATES/%'
    #         or path LIKE '%/RT_VEHICLE_POSITIONS/%'
    #     )
    #     ORDER BY created_on;

    update_md_query = """
            UPDATE
                metadata_log
            SET
                rail_pm_process_fail = false
                , rail_pm_processed = false
            WHERE
                substring(path, '\d{4}-\d{2}-\d{2}')::date >= '2025-04-22'
                and substring(path, '\d{4}-\d{2}-\d{2}')::date <= '2025-04-23'
                and (
                    path LIKE '%/RT_TRIP_UPDATES/%'
                    or path LIKE '%/RT_VEHICLE_POSITIONS/%'
                )
            ;
            """
    op.execute(update_md_query)


def downgrade() -> None:
    pass
