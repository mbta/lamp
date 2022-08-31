import logging
import time
import zipfile

from typing import IO, List, Union

import pyarrow
from pyarrow import csv

from .s3_utils import get_zip_buffer
from .converter import Converter


def zip_to_pyarrow(filename: str) -> List[tuple[str, pyarrow.Table]]:
    """
    convert a schedule gtfs zip file into tables. the zip file is essentially a
    small database with each contained file (outside of feed info) acting as its
    own table. info on the gtfs scheduling standard can be found at
    http://gtfs.org/schedule/
    """
    start_time = time.time()
    logging.info(
        "start=%s, filename=%s",
        "convert_single_gtfs",
        filename,
    )

    try:
        tables = []
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
                tables.append((filename_prefix, table))

        logging.info(
            "complete=%s, duration=%.2f, filename=%s",
            "convert_single_gtfs",
            start_time - time.time(),
            filename,
        )
        return tables
    except Exception as exception:
        logging.exception(
            "failed=%s, error_type=%s, filename=%s",
            "convert_single_gtfs",
            type(exception).__name__,
            filename,
        )
        return []


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def convert(self, files: list[str]) -> list[tuple[str, pyarrow.Table]]:
        start_time = time.time()
        logging.info(
            "start=%s, filecount=%d",
            "convert_gtfs",
            len(files),
        )

        all_tables = []
        for file in files:
            tables = list(zip_to_pyarrow(file))

            if len(tables) == 0:
                self.error_files.append(file)
            else:
                self.archive_files.append(file)
                all_tables += tables

        logging.info(
            "complete=%s, duration=%.2f",
            "convert_gtfs",
            start_time - time.time(),
        )
        return all_tables

    @property
    def partition_cols(self) -> list[str]:
        return ["timestamp"]
