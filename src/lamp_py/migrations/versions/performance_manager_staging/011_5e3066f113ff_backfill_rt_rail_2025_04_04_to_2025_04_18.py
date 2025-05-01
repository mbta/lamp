"""update_glides_location_column_names

Revision ID: 5e3066f113ff
Revises: 36e7a7aee148
Create Date: Wed Apr 23 11:16:12 EDT 2025

Details
This will clean up missing data from RDS performance issues/outage from 4/14-4/17
This will also clean up duplication of data in prod from 4/17-4/22

* upgrade -> Delete all records from 4/4 to 4/23 in vehicle events and vehicle_trips
          -> Set all flags to "unprocessed" in metadata log from 4/4 to 4/22
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
revision = "5e3066f113ff"
down_revision = "36e7a7aee148"
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
