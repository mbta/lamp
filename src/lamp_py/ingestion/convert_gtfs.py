import os
import zipfile
from typing import (
    List,
    Tuple,
)

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
from lamp_py.runtime_utils.remote_files import (
    S3_SPRINGBOARD,
    LAMP,
)
from .converter import Converter


def gtfs_files_to_convert() -> List[Tuple[str, int]]:
    """
    create list of Tuple[GTFS url, version_key] for GtfsConverter

    version_key is based on published_dt
    """
    mbta_schedule_feed = ordered_schedule_frame()

    last_s3_pq = file_list_from_s3(
        bucket_name=S3_SPRINGBOARD,
        file_prefix=f"{LAMP}/FEED_INFO/",
    )[-1:]
    if len(last_s3_pq) > 0:
        last_s3_df = pl.read_parquet(last_s3_pq[0])

        mbta_schedule_feed = mbta_schedule_feed.filter(
            pl.col("feed_start_date") >= last_s3_df.item(0, "feed_start_date"),
            pl.col("feed_version") != last_s3_df.item(0, "feed_version"),
        )

    # add version_key column
    mbta_schedule_feed = mbta_schedule_feed.with_columns(
        pl.col("published_dt")
        .dt.timestamp("ms")
        .floordiv(1000)
        .alias("version_key")
    )

    return mbta_schedule_feed.select(["archive_url", "version_key"]).rows(
        named=False
    )


class GtfsConverter(Converter):
    """
    Converter for GTFS Schedule Data
    """

    def convert(self) -> None:
        for url, version_key in gtfs_files_to_convert():
            process_logger = ProcessLogger(
                "parquet_table_creator",
                table_type="gtfs",
                url=url,
                version_key=version_key,
            )
            process_logger.log_start()
            try:
                self.process_schedule(url, version_key)
                process_logger.log_complete()

            except Exception as exception:
                process_logger.log_failure(exception)

    def process_schedule(self, url: str, version_key: int) -> None:
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
                S3_SPRINGBOARD,
                LAMP,
                s3_prefix,
            ),
            partition_cols=["timestamp"],
            visitor_func=visitor_func,
        )
