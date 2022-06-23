import zipfile

from collections.abc import Iterable
from typing import IO, Union

import pyarrow as pa
from pyarrow import csv

from .s3_utils import get_zip_buffer


def zip_to_pyarrow(filename: str) -> Iterable[tuple[str, pa.Table]]:
    """
    convert a schedule gtfs zip file into tables. the zip file is essentially a
    small database with each contained file (outside of feed info) acting as its
    own table. info on the gtfs scheduling standard can be found at
    http://gtfs.org/schedule/
    """
    filelike_input: Union[str, IO[bytes]] = filename
    last_modified = 0
    if filename.startswith("s3://"):
        file_to_load = str(filename).replace("s3://", "")
        filelike_input, last_modified = get_zip_buffer(file_to_load)

    with zipfile.ZipFile(filelike_input) as gtfs_zip:
        # iterate over each "table"
        for gtfs_filename in gtfs_zip.namelist():
            with gtfs_zip.open(gtfs_filename) as file:
                table = csv.read_csv(file)

                # add the last modified timestamp
                timestamp = [last_modified] * table.num_rows
                table = table.append_column("timestamp", [timestamp])

            filename_prefix = gtfs_filename.replace(".txt", "").upper()
            yield (filename_prefix, table)
