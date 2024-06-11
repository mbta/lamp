import re
import pathlib
import datetime
import zoneinfo
from typing import Dict, List
from urllib import request
from io import BytesIO

import polars as pl

import pyarrow
import pyarrow.compute as pc

DEFAULT_S3_PREFIX = "lamp"


def group_sort_file_list(filepaths: List[str]) -> Dict[str, List[str]]:
    """
    group and sort list of filepaths by filename

    expects s3 file paths that can be split on timestamp:

    full_path:
    s3://mbta-ctd-dataplatform-dev-incoming/lamp/delta/2022/10/12/2022-10-12T23:58:52Z_https_cdn.mbta.com_MBTA_GTFS.zip

    splits "2022-10-12T23:58:52Z_https_cdn.mbta.com_MBTA_GTFS.zip"
    from full_path

    into
     - 2022-10-12T23:58:52Z
     - https_cdn.mbta.com_MBTA_GTFS.zip

    groups by "https_cdn.mbta.com_MBTA_GTFS.zip"
    """

    def strip_timestamp(fileobject: str) -> str:
        """
        utility for sorting pulling timestamp string out of file path.
        assumption is that the objects will have a bunch of "directories" that
        pathlib can parse out, and the filename will start with a timestamp
        "YYY-MM-DDTHH:MM:SSZ" (20 char) format.

        This utility will be used to sort the list of objects.
        """
        filepath = pathlib.Path(fileobject)
        return filepath.name[:20]

    grouped_files: Dict[str, List[str]] = {}

    for file in filepaths:
        # skip filepaths that are directories.
        if file[-1] == "/":
            continue

        _, file_type = pathlib.Path(file).name.split("_", maxsplit=1)

        if file_type not in grouped_files:
            grouped_files[file_type] = []

        grouped_files[file_type].append(file)

    for group in grouped_files.values():
        group.sort(key=strip_timestamp)

    return grouped_files


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
        feed = pl.read_csv(
            res.read(), columns=feed_columns, schema_overrides=feed_dtypes
        )

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


def flatten_schema(table: pyarrow.table) -> pyarrow.table:
    """flatten pyarrow table if struct column type exists"""
    for field in table.schema:
        if str(field.type).startswith("struct"):
            return flatten_schema(table.flatten())
    return table


def explode_table_column(table: pyarrow.table, column: str) -> pyarrow.table:
    """explode list-like column of pyarrow table by creating rows for each list value"""
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    indices = pc.list_parent_indices(table[column])
    return pyarrow.concat_tables(
        [
            table.select(other_columns)
            .take(indices)
            .append_column(
                pyarrow.field(
                    column, table.schema.field(column).type.value_type
                ),
                pc.list_flatten(table[column]),
            ),
            table.filter(pc.list_value_length(table[column]).is_null()).select(
                other_columns
            ),
        ],
        promote=True,
    )
