import os
import zipfile
from datetime import datetime
from typing import List

from pyarrow import csv
import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import (
    write_parquet_file,
    file_list_from_s3,
)
from lamp_py.ingestion.utils import (
    ordered_schedule_frame,
    file_as_bytes_buf,
)
from .converter import Converter
from .utils import DEFAULT_S3_PREFIX


def gtfs_files_to_convert() -> List[str]:
    """
    create list of GTFS url paths for GtfsConverter
    """
    mbta_schedule_feed = ordered_schedule_frame()

    last_s3_pq = file_list_from_s3(
        bucket_name=os.getenv("SPRINGBOARD_BUCKET"),
        file_prefix="lamp/FEED_INFO/",
    )[-1:]
    if len(last_s3_pq) > 0:
        last_s3_df = pl.read_parquet(last_s3_pq[0])

        mbta_schedule_feed = mbta_schedule_feed.filter(
            pl.col("feed_start_date") >= last_s3_df.item(0, "feed_start_date"),
            pl.col("feed_version") != last_s3_df.item(0, "feed_version"),
        )

    return mbta_schedule_feed.get_column("archive_url").to_list()


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def convert(self) -> None:
        for url in gtfs_files_to_convert():
            process_logger = ProcessLogger(
                "parquet_table_creator", table_type="gtfs", url=url
            )
            process_logger.log_start()
            try:
                self.process_schedule(url)
                process_logger.log_complete()

            except Exception as exception:
                process_logger.log_failure(exception)

    def process_schedule(self, url: str) -> None:
        """
        convert a schedule gtfs zip file into tables. the zip file is
        essentially a small database with each contained file (outside of feed
        info) acting as its own table. info on the gtfs scheduling standard can
        be found at http://gtfs.org/schedule/
        """
        # schedule objects are read directly from url as a stream of bytes
        filelike_input = file_as_bytes_buf(url)

        # open up the static schedule and iterate over each of its "tables"
        with zipfile.ZipFile(filelike_input) as gtfs_zip:
            # get the time the feed info file was last modified.
            # ZipInfo.date_time returns a tuple that can be unpacked to create
            # a datetime which is used to create a posix integer timetstamp.
            # this timestamp is used as a version key to link all of the
            # schedule data together.
            written_time = gtfs_zip.getinfo("feed_info.txt").date_time
            version_key = int(datetime(*written_time).timestamp())

            for gtfs_filename in gtfs_zip.namelist():
                # performance manager kicks off its processing of the static
                # schedule when the feed info file is added to the metadata
                # log. if that file is added, but other files from the static
                # schedule have not been written, this will cause an error.
                # ingest and write the feed info table last.
                if gtfs_filename == "feed_info.txt":
                    continue
                self.create_table(gtfs_zip, gtfs_filename, version_key)

            self.create_table(gtfs_zip, "feed_info.txt", version_key)

    def create_table(
        self, gtfs_zip: zipfile.ZipFile, table_filename: str, version_key: int
    ) -> None:
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

        # if the table has no rows in it, early exit. the write_parquet_file
        # will throw if an empty table is passed in.
        if table.num_rows == 0:
            return

        visitor_func = None
        if s3_prefix == "FEED_INFO":
            visitor_func = self.send_metadata

        write_parquet_file(
            table=table,
            file_type=s3_prefix,
            s3_dir=os.path.join(
                os.environ["SPRINGBOARD_BUCKET"],
                DEFAULT_S3_PREFIX,
                s3_prefix,
            ),
            partition_cols=["timestamp"],
            visitor_func=visitor_func,
        )
