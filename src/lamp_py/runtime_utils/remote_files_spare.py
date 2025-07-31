import os
from dataclasses import dataclass
from typing import Union

from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3_SPRINGBOARD, S3Location

# prefix constants
SPARE = "spare"
SPARE_PROCESSED = os.path.join(SPARE, "processed")
SPARE_TABLEAU = os.path.join(SPARE, "tableau")

VERSION_KEY = "spare_version"

# files ingested from SPARE
springboard_spare_vehicles = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(SPARE, "vehicles.parquet"),
)

# files read temp from SPARE
archive_spare_vehicles = S3Location(
    bucket=S3_ARCHIVE,
    prefix=os.path.join(SPARE_PROCESSED, "vehicles.parquet"),
)

# published by SPARE

tableau_spare_vehicles = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "spare_vehicles.parquet"))
