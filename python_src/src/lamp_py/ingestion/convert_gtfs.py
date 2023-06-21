import os
import zipfile
from typing import IO, List, Union

import pyarrow
from pyarrow import csv

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import (
    get_zip_buffer,
    move_s3_objects,
    write_parquet_file,
)

from .converter import Converter
from .utils import DEFAULT_S3_PREFIX


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def convert(self) -> List[str]:
        archive_files = []
        error_files = []

        for file in self.files:
            process_logger = ProcessLogger(
                "parquet_table_creator", table_type="gtfs", filename=file
            )
            process_logger.log_start()
            try:
                self.process_schedule(file)
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

        return error_files + archive_files

    def process_schedule(self, filename: str) -> None:
        """
        convert a schedule gtfs zip file into tables. the zip file is
        essentially a small database with each contained file (outside of feed
        info) acting as its own table. info on the gtfs scheduling standard can
        be found at http://gtfs.org/schedule/
        """
        # s3 objects are read directly from s3 as a stream of bytes
        filelike_input: Union[str, IO[bytes]] = filename

        # the last modified date is pulled from s3 and we use it as our version
        # key to link all of the table data together
        last_modified = 0

        if filename.startswith("s3://"):
            file_to_load = str(filename).replace("s3://", "")
            filelike_input, last_modified = get_zip_buffer(file_to_load)

        # open up the static schedule and iterate over each of its "tables"
        with zipfile.ZipFile(filelike_input) as gtfs_zip:
            for gtfs_filename in gtfs_zip.namelist():
                # performance manager kicks off its processing of the static
                # schedule when the feed info file is added to the metadata
                # log. if that file is added, but other files from the static
                # schedule have not been written, this will cause an error.
                # ingest and write the feed info table last.
                if gtfs_filename == "feed_info.txt":
                    continue
                self.create_table(gtfs_zip, gtfs_filename, last_modified)

            self.create_table(gtfs_zip, "feed_info.txt", last_modified)

    def create_table(
        self, gtfs_zip: zipfile.ZipFile, table_filename: str, version_key: int
    ) -> pyarrow.Table:
        """
        read a csv table out of a gtfs static schedule file, add a timestamp
        column to each row, and write it as a parquet file on s3, partitioned
        by the timestamp

        @param gtfs_zip - the opened zip file
        @param table_filename - name of the table to read, convert, and write
        @param timestamp - timestamp info to add to reach row. this timestamp
            value can be used as a version key to link all of the schedule
            data
        """
        with gtfs_zip.open(table_filename) as table_file:
            table = csv.read_csv(table_file)

            # add the last modified timestamp
            version_column = [version_key] * table.num_rows
            table = table.append_column("timestamp", [version_column])

        s3_prefix = table_filename.replace(".txt", "").upper()

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
