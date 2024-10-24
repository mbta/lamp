"""update_glides_location_column_names

Revision ID: 26db393ea854
Revises: 07903947aabe
Create Date: 2024-07-09 12:12:04.325358

Details
* upgrade -> for each glides parquet file:
    * rename columns to match api. replace gtfsID with gtfsId and todsID with
        todsId
    * unique each dataset based on the 'id' uuid field.

* downgrade -> Nothing
"""

import os
import tempfile
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List

from lamp_py.aws.s3 import download_file, upload_file

# revision identifiers, used by Alembic.
revision = "26db393ea854"
down_revision = "07903947aabe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    def update_glides_archive(temp_dir: str, base_filename: str) -> None:
        """
        * download the remote file to a local temp dir
        * rename columns with "gtfsID" or "todsID" in them to use "Id"
        * unique columns
        * sort the dataset based on 'time' column
        """
        remote_path = f"s3://{os.environ['SPRINGBOARD_BUCKET']}/lamp/GLIDES/{base_filename}"
        old_local_path = os.path.join(temp_dir, f"old_{base_filename}")
        new_local_path = os.path.join(temp_dir, f"new_{base_filename}")

        file_exists = download_file(remote_path, old_local_path)
        if not file_exists:
            return

        old_table = pq.read_table(old_local_path)

        # build the new schema by converting names and keeping types
        fields: List[pa.Field] = []
        for column in old_table.schema:
            if "gtfsID" in column.name:
                new_name = column.name.replace("gtfsID", "gtfsId")
                new_field = pa.field(new_name, column.type)
                fields.append(new_field)
            elif "todsID" in column.name:
                new_name = column.name.replace("todsID", "todsId")
                new_field = pa.field(new_name, column.type)
                fields.append(new_field)
            else:
                fields.append(column)

        schema = pa.schema(fields)

        # rename columns to match new schema
        # unique the records
        # cast to new schema (polars converts things)
        new_table = (
            pl.DataFrame(old_table.rename_columns(schema.names)).unique().sort(by=["time"]).to_arrow().cast(schema)
        )

        pq.write_table(new_table, new_local_path)
        upload_file(new_local_path, remote_path)

    files_to_update = [
        "editor_changes.parquet",
        "operator_sign_ins.parquet",
        "trip_updates.parquet",
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        for filename in files_to_update:
            update_glides_archive(temp_dir, filename)


def downgrade() -> None:
    pass
