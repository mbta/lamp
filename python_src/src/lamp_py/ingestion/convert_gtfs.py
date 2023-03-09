import os
import zipfile
from typing import IO, List, Tuple, Union

import pyarrow
from pyarrow import csv

from lamp_py.aws.s3 import (
    get_zip_buffer,
    move_s3_objects,
    write_parquet_file,
)
from lamp_py.logging_utils import ProcessLogger

from .converter import Converter
from .utils import DEFAULT_S3_PREFIX


def zip_to_pyarrow(filename: str) -> List[Tuple[str, pyarrow.Table]]:
    """
    convert a schedule gtfs zip file into tables. the zip file is
    essentially a small database with each contained file (outside of feed
    info) acting as its own table. info on the gtfs scheduling standard can
    be found at http://gtfs.org/schedule/
    """
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

        return tables


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def convert(self) -> None:
        archive_files = []
        error_files = []

        for file in self.files:
            process_logger = ProcessLogger(
                "parquet_table_creator", table_type="gtfs", filename=file
            )
            process_logger.log_start()
            try:
                tables = zip_to_pyarrow(file)

                for s3_prefix, table in tables:
                    write_parquet_file(
                        table=table,
                        config_type=s3_prefix,
                        s3_path=os.path.join(
                            os.environ["SPRINGBOARD_BUCKET"],
                            DEFAULT_S3_PREFIX,
                            s3_prefix,
                        ),
                        partition_cols=["timestamp"],
                        visitor_func=self.send_metadata,
                    )

                archive_files.append(file)
                process_logger.log_complete()

            except Exception as exception:
                error_files.append(file)
                process_logger.log_failure(exception)

        if len(error_files) > 0:
            move_s3_objects(
                error_files,
                os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
            )

        if len(archive_files) > 0:
            move_s3_objects(
                archive_files,
                os.path.join(os.environ["ARCHIVE_BUCKET"], DEFAULT_S3_PREFIX),
            )
