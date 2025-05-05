import os
import datetime


def pick_n_directories(options: list[str]) -> list[str]:
    """
    Given a list of directories, show them to user to select the set of directories to return as a list
    """

    print("Select options (enter numbers separated by spaces):")
    for i, option in enumerate(options):
        print(f"{i + 1}. {option}")

    while True:
        try:
            choices = input("> ")
            selected_indices = [int(c) - 1 for c in choices.split()]
            if not all(0 <= i < len(options) for i in selected_indices):
                raise ValueError
            return [options[i] for i in selected_indices]
        except ValueError:
            print("Invalid input. Please enter numbers separated by spaces, corresponding to the options.")


def migration_template(
    current_id: str,
    previous_id: str,
    date_string: str,
    alembic_string: str,
    detail_desc: str,
    upgrade_desc: str,
    downgrade_desc: str,
) -> str:
    """
    Fillable template for a generic migration. This gets populated and
    filled out with directory dependent curr/prev id and data. WIP
    """
    return f'''"""{alembic_string}

    Revision ID: {current_id}
    Revises: {previous_id}
    Create Date: {date_string}

    Details: {detail_desc}

    * upgrade -> {upgrade_desc}
    * downgrade -> {downgrade_desc}
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
revision = "{current_id}"
down_revision = "{previous_id}"
branch_labels = None # tbd
depends_on = None #tbd


def upgrade() -> None:
    pass

def downgrade() -> None:
    pass
    '''


if __name__ == "__main__":
    import uuid

    short_desc = "reprocess_422_423"
    uuid_new = uuid.uuid4().hex[-12:]

    versions_dir = "/Users/hhuang/lamp/lamp/src/lamp_py/migrations/versions"

    # List directories in the versions directory
    if os.path.exists(versions_dir):
        directories = sorted([d for d in os.listdir(versions_dir) if os.path.isdir(os.path.join(versions_dir, d))])
        print("Directories in 'versions':", directories)
    else:
        print(f"The directory '{versions_dir}' does not exist.")

    options = pick_n_directories(directories)

    print(options)
    for o in options:
        latest_migration = sorted([d for d in os.listdir(os.path.join(versions_dir, o)) if not d.startswith("sql")])[-1]
        parts = os.path.basename(latest_migration).split("_")
        breakpoint()
        increment_migration_count = str(int(parts[0]) + 1).zfill(3)
        uuid_prev = parts[1]

        with open(f"{versions_dir}/{o}/{increment_migration_count}_{uuid_new}_{short_desc}.py", "w") as f:
            f.write(
                migration_template(
                    current_id=uuid_new,
                    previous_id=uuid_prev,
                    alembic_string=short_desc,
                    date_string=str(datetime.datetime.now()),
                    detail_desc="FILL ME IN",
                    upgrade_desc="test upgrade",
                    downgrade_desc="None",
                )
            )
