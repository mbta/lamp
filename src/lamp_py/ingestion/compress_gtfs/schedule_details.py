import os
import csv
import zipfile
import datetime

from typing import List
from io import BytesIO
from io import TextIOWrapper
from dataclasses import dataclass
from dataclasses import field

import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.ingestion.utils import (
    ordered_schedule_frame,
    file_as_bytes_buf,
)
from lamp_py.ingestion.compress_gtfs.gtfs_schema_map import gtfs_schema
from lamp_py.aws.s3 import (
    file_list_from_s3,
    download_file,
)
from lamp_py.runtime_utils.remote_files import compressed_gtfs


# pylint: disable=R0902
# Too many instance attributes
@dataclass
class ScheduleDetails:
    """
    GTFS schedule details required to create compressed parquet files

    publish date of a schedule is assumed to be the date embeded in the
    `feed_version` field of the `feed_info.txt` table

    GTFS schedule is assumed to be `active_from` the day after the publish date

    the purpose of the `active_to` date is to just bridge the gap until the
    next sequential schedule becomes active. the +365 days is an arbitrarily
    long duration, during which, another schedule should be issued
    """

    file_location: str
    published_dt: datetime.datetime
    tmp_folder: str

    gtfs_bytes: BytesIO = field(init=False)
    file_list: List[str] = field(init=False)

    published_int: int = field(init=False)
    active_from_int: int = field(init=False)
    active_to_int: int = field(init=False)

    def __post_init__(self) -> None:
        self.gtfs_bytes = file_as_bytes_buf(self.file_location)

        with zipfile.ZipFile(self.gtfs_bytes) as zf:
            self.file_list = [file.filename for file in zf.filelist]

        active_from_dt = self.published_dt + datetime.timedelta(days=1)
        active_to_dt = self.published_dt + datetime.timedelta(days=365)

        strf_fmt = "%Y%m%d"
        self.published_int = int(self.published_dt.strftime(strf_fmt))
        self.active_to_int = int(active_to_dt.strftime(strf_fmt))
        self.active_from_int = int(active_from_dt.strftime(strf_fmt))

    def headers_from_file(self, gtfs_table_file: str) -> List[str]:
        """
        extract header columns from gtfs_table_file

        :param gtfs_table_file (ie. stop_times.txt)

        :return List[header_names]
        """
        if gtfs_table_file not in self.file_list:
            raise KeyError(f"{gtfs_table_file} not found in {self.file_location} archive")

        with zipfile.ZipFile(self.gtfs_bytes) as zf:
            with zf.open(gtfs_table_file) as f_bytes:
                with TextIOWrapper(f_bytes, encoding="utf8") as f_text:
                    reader = csv.reader(f_text)
                    return next(reader)

    def gtfs_to_frame(self, gtfs_table_file: str) -> pl.DataFrame:
        """
        create frame from .txt gtfs table

        dataframe will include all columns that are defined in polars_schema_map for gtfs_table_file
        if defined columns are missing from .txt table they are added with all NULL values
        "from_zip":bool column added as flag for merge operations

        if gtfs_table_file does not exist in zip archive, empty dataframe with
        expected schema is returned

        these "new" records will always have:
            - "gtfs_active_date" = self.active_from_int
            - "gtfs_end_date" = self.active_to_int

        :param gtfs_table_file (ie. stop_times.txt)

        :return gtfs_table_file as polars DataFrame
        """
        logger = ProcessLogger(
            "gtfs_to_frame",
            schedule=self.file_location,
            table_file=gtfs_table_file,
        )
        logger.log_start()
        table_schema = gtfs_schema(gtfs_table_file)

        if gtfs_table_file not in self.file_list:
            logger.add_metadata(table_not_in_archive=True)

            frame = pl.DataFrame(schema=table_schema)
            frame = frame.with_columns(
                pl.lit(True).cast(pl.Boolean).alias("from_zip"),
                pl.lit(self.active_from_int).alias("gtfs_active_date"),
                pl.lit(self.active_to_int).alias("gtfs_end_date"),
            )
            logger.log_complete()
            return frame

        expected_columns = set(table_schema.keys())
        columns_in_zip = set(self.headers_from_file(gtfs_table_file))

        columns_to_pull = list(expected_columns.intersection(columns_in_zip))
        dtypes_to_pull = {col: table_schema[col] for col in columns_to_pull}

        with zipfile.ZipFile(self.gtfs_bytes) as zfile:
            with zfile.open(gtfs_table_file) as f:
                frame = pl.read_csv(
                    f.read(),
                    columns=columns_to_pull,
                    schema_overrides=dtypes_to_pull,
                    has_header=True,
                )

        # log missing columns
        missing_columns = expected_columns.difference(columns_in_zip)
        if missing_columns:
            logger.add_metadata(
                missing_columns_count=len(missing_columns),
                missing_columns=",".join(missing_columns),
            )

        # log unexpected columns
        unexpected_columns = columns_in_zip.difference(expected_columns)
        if unexpected_columns:
            logger.add_metadata(
                unexpected_columns_count=len(unexpected_columns),
                unexpected_columns=",".join(unexpected_columns),
            )

        # add missing columns as all NULL values
        for null_col in missing_columns:
            frame = frame.with_columns(pl.lit(None).cast(table_schema[null_col]).alias(null_col))

        # update String values containing only spaces to NULL
        frame = frame.with_columns(
            pl.when(pl.col(pl.Utf8).str.replace(r"\s*", "", n=1).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.Utf8))
            .name.keep()
        )

        # add "from_zip" (True) for merge operation with parquet and date columns
        # de-duplicate.... just in case
        frame = frame.with_columns(
            pl.lit(True).cast(pl.Boolean).alias("from_zip"),
            pl.lit(self.active_from_int).alias("gtfs_active_date"),
            pl.lit(self.active_to_int).alias("gtfs_end_date"),
        ).unique()

        logger.log_complete()

        return frame


# pylint: enable=R0902


def schedules_to_compress(tmp_folder: str) -> pl.DataFrame:
    """
    compare already compressed schedule files to schedules available in the MBTA
    feed archive (https://cdn.mbta.com/archive/archived_feeds.txt) to determine
    which schedules need to be compressed

    if compressed schedule files exist locally, comparison will be made against
    feed_info.parquet file

    if no local compressed schedules are found, sync will be attempted with S3 bucket

    :return frame of schedules needing to be compresesed, with schema:
    {
        feed_start_date: int,
        feed_version: str,
        archive_url: str,
        published_dt: datetime.datetime, (published date extracted from feed_version)
        published_date: int, (published_dt date as YYYYMMDD int)
    }
    """
    feed = ordered_schedule_frame()

    feed_years = sorted(
        feed["published_dt"].dt.strftime("%Y").unique(),
        reverse=True,
    )

    # Insert extra year in case schedule is issued on last day of year
    feed_years.insert(0, str(int(feed_years[0]) + 1))

    for year in feed_years:
        if not os.path.exists(os.path.join(tmp_folder, year)):
            os.makedirs(os.path.join(tmp_folder, year))

        pq_fi_path = os.path.join(tmp_folder, year, "feed_info.parquet")
        if not os.path.exists(pq_fi_path):
            prefix = os.path.join(compressed_gtfs.prefix, year)
            s3_files = file_list_from_s3(compressed_gtfs.bucket, prefix)
            if len(s3_files) > 1:
                for obj_path in s3_files:
                    if not obj_path.endswith(".parquet"):
                        continue
                    local_path = obj_path.replace(f"s3://{compressed_gtfs.bucket}", "/tmp")
                    download_file(obj_path, local_path)
            else:
                continue

        pq_fi_frame = pl.read_parquet(pq_fi_path)

        if int(year) <= 2018:
            # different filter operation used for less than year 2018 because some
            # schedules in this date range do not have matching "feed_version"
            # values between `feed_info` file in schedule and "archived_feeds.txt" file
            feed = feed.filter(
                (pl.col("published_date") > int(f"{year}0000"))
                & (pl.col("feed_start_date") > pq_fi_frame.get_column("feed_start_date").max())
            )
        else:
            # anti join against records for 'year' to find records not already in feed_info.parquet
            feed = feed.filter(pl.col("published_date") > int(f"{year}0000")).join(
                pq_fi_frame.select("feed_version"),
                on="feed_version",
                how="anti",
                coalesce=True,
            )

        break

    return feed
