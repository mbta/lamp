"""regen-rail-perf-20260101-to-20260104

    Revision ID: f138635d1338
    Revises: 5e3066f113ff
    Create Date: 2026-02-28 11:31:59.737856

    Details: regen-rail-perf-20260101-to-20260104

    * upgrade -> test upgrade
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
revision = "f138635d1338"
down_revision = "5e3066f113ff"
branch_labels = None # tbd
depends_on = None #tbd


def upgrade() -> None:
    pass

def downgrade() -> None:
    pass
    