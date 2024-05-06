import re
import os
import csv
import zipfile
import datetime
import zoneinfo

from urllib import request
from typing import List
from io import BytesIO
from io import TextIOWrapper
from dataclasses import dataclass
from dataclasses import field

import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_schema_map import gtfs_schema


def file_as_bytes_buf(file: str) -> BytesIO:
    """
    Create buffer of local or http(s) file path

    :return BytesIO buffer
    """
    if file.startswith("http"):
        with request.urlopen(file) as response:
            return BytesIO(response.read())

    with open(file, "rb") as f:
        return BytesIO(f.read())


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
            raise KeyError(
                f"{gtfs_table_file} not found in {self.file_location} archive"
            )

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
                    dtypes=dtypes_to_pull,
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
            frame = frame.with_columns(
                pl.lit(None).cast(table_schema[null_col]).alias(null_col)
            )

        # update String values containing only spaces to NULL
        frame = frame.with_columns(
            pl.when(
                pl.col(pl.Utf8).str.replace(r"\s*", "", n=1).str.len_chars()
                == 0
            )
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


def date_from_feed_version(feed_version: str) -> datetime.datetime:
    """
    Extract date from feed_version text. Raise LookupError if no date found.

    known date formats:
        - YYYY-MM-DD
        - MM/DD/YY
    YYYY-MM-DD iso string will be converted from UTC to US/Eastern

    :param feed_version: feed_version of gtfs schedule

    :return: datetime extracted from feed_version text
    """
    utc_tz = datetime.timezone.utc
    local_tz = zoneinfo.ZoneInfo("US/Eastern")

    pattern_1 = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    pattern_1_result = re.search(pattern_1, feed_version)

    pattern_2 = r"(\d{1,2}\/\d{1,2}\/\d{2})"
    pattern_2_result = re.search(pattern_2, feed_version)

    if pattern_1_result is not None:
        date_str = pattern_1_result.group(0)
        date_dt = datetime.datetime.fromisoformat(date_str).replace(
            tzinfo=utc_tz
        )
        date_dt = date_dt.astimezone(local_tz).replace(tzinfo=None)
    elif pattern_2_result is not None:
        date_str = pattern_2_result.group(0)
        date_dt = datetime.datetime.strptime(date_str, "%m/%d/%y")
    else:
        raise LookupError(f"No date found in feed_version: '{feed_version}'")

    return date_dt


def ordered_schedule_frame() -> pl.DataFrame:
    """
    create de-duplicated and ordered frame of all MBTA gtfs schedules from
    https://cdn.mbta.com/archive/archived_feeds.txt

    de-duplicated on: published_date
    ordered: oldest -> newest

    :return frame with schema:
    {
        feed_start_date: int,
        feed_version: str,
        archive_url: str,
        published_dt: datetime.datetime, (published date extracted from feed_version)
        published_date: int, (published_dt date as YYYYMMDD int)
    }
    """
    archive_url = "https://cdn.mbta.com/archive/archived_feeds.txt"
    feed_columns = (
        "feed_start_date",
        "feed_version",
        "archive_url",
    )
    feed_dtypes = {
        "feed_start_date": pl.Int32,
        "feed_version": pl.String,
        "archive_url": pl.String,
    }

    # Accept-Encoding header required to avoid cloudfront cache-hit
    req = request.Request(archive_url, headers={"Accept-Encoding": "gzip"})
    with request.urlopen(req) as res:
        feed = pl.read_csv(res.read(), columns=feed_columns, dtypes=feed_dtypes)

    feed = (
        feed.with_columns(
            pl.col("feed_version")
            .map_elements(date_from_feed_version, pl.Datetime)
            .alias("published_dt"),
        )
        .with_columns(
            pl.col("published_dt")
            .dt.strftime("%Y%m%d")
            .cast(pl.Int32)
            .alias("published_date"),
        )
        .sort(
            by=["feed_start_date", "published_dt"],
        )
        .unique(
            subset="published_date",
            keep="last",
        )
        .sort(
            by="published_dt",
        )
    )

    # fix_me: filter for malformed archive schedules
    feed = feed.filter(feed["feed_start_date"] > 20180200)

    return feed


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

    for year in feed_years:
        if not os.path.exists(os.path.join(tmp_folder, year)):
            os.makedirs(os.path.join(tmp_folder, year))

        pq_fi_path = os.path.join(tmp_folder, year, "feed_info.parquet")
        if not os.path.exists(pq_fi_path):
            # check for file in s3_path...
            continue

        pq_fi_frame = pl.read_parquet(pq_fi_path)

        # anti join against records for 'year' to find records not already in feed_info.parquet
        feed = feed.filter(pl.col("published_date") > int(f"{year}0000")).join(
            pq_fi_frame.select("feed_version"), on="feed_version", how="anti"
        )

        break

    return feed