import os
import tempfile
import datetime
from unittest import mock

import pyarrow.compute as pc
import pyarrow.dataset as pd
import polars as pl

from lamp_py.ingestion.compress_gtfs.schedule_details import (
    ScheduleDetails,
    schedules_to_compress,
)
from lamp_py.ingestion.compress_gtfs.gtfs_to_parquet import (
    compress_gtfs_schedule,
)
from lamp_py.ingestion.compress_gtfs.gtfs_schema_map import gtfs_schema_list
from lamp_py.ingestion.compress_gtfs.pq_to_sqlite import pq_folder_to_sqlite


# pylint: disable=R0914
def test_gtfs_to_parquet_compression() -> None:
    """
    test gtfs -> parquet compression pipeline

    will test compression of 3 randomly selected schedules from the past year
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        with mock.patch(
            "lamp_py.ingestion.compress_gtfs.schedule_details.file_list_from_s3"
        ) as patch_s3:
            patch_s3.return_value = []
            feed = schedules_to_compress(temp_dir)
            patch_s3.assert_called()

        year_ago = datetime.datetime.now() - datetime.timedelta(weeks=52)

        feed = (
            feed.filter(pl.col("published_dt") > year_ago)
            .sample(n=3)
            .sort(by="published_dt")
            .with_columns(
                pl.col("published_date").shift(n=-1).alias("active_end"),
            )
        )

        # compress each schedule in test feed
        for schedule in feed.rows(named=True):
            schedule_details = ScheduleDetails(
                schedule["archive_url"],
                schedule["published_dt"],
                temp_dir,
            )
            compress_gtfs_schedule(schedule_details)

        # verify sqlite db creation and gzip for 1 year
        year = feed["published_dt"].dt.strftime("%Y").unique()[0]
        year_path = os.path.join(temp_dir, year)
        pq_folder_to_sqlite(year_path)
        assert os.path.exists(os.path.join(year_path, "GTFS_ARCHIVE.db.gz"))

        # check parquet file exports
        for schedule in feed.rows(named=True):
            schedule_url = schedule["archive_url"]
            schedule_details = ScheduleDetails(
                schedule_url,
                schedule["published_dt"],
                temp_dir,
            )
            active_start_date = schedule_details.active_from_int
            active_end_date = schedule["active_end"]
            if active_end_date is None:
                # last day of year
                active_end_date = int(f"{str(active_start_date)[:4]}1231")

            for gtfs_file in gtfs_schema_list():
                zip_count = schedule_details.gtfs_to_frame(gtfs_file).shape[0]

                # check start of parquet schedule
                start_path = os.path.join(
                    temp_dir,
                    f"{str(active_start_date)[:4]}",
                    gtfs_file.replace(".txt", ".parquet"),
                )
                pq_start_count = 0
                if os.path.exists(start_path):
                    pq_filter = (
                        pc.field("gtfs_active_date") <= active_start_date
                    ) & (pc.field("gtfs_end_date") >= active_start_date)
                    pq_start_count = (
                        pd.dataset(start_path).filter(pq_filter).count_rows()
                    )

                assert (
                    pq_start_count == zip_count
                ), f"{schedule_url=} {gtfs_file=} {active_start_date=} {active_end_date=}"

                # check end of parquet schedule
                end_path = os.path.join(
                    temp_dir,
                    f"{str(active_end_date)[:4]}",
                    gtfs_file.replace(".txt", ".parquet"),
                )
                pq_end_count = 0
                if os.path.exists(end_path):
                    pq_filter = (
                        pc.field("gtfs_active_date") <= active_end_date
                    ) & (pc.field("gtfs_end_date") >= active_end_date)
                    pq_end_count = (
                        pd.dataset(end_path).filter(pq_filter).count_rows()
                    )

                assert (
                    pq_end_count == zip_count
                ), f"{schedule_url=} {gtfs_file=} {active_start_date=} {active_end_date=}"


# pylint: enable=R0914
