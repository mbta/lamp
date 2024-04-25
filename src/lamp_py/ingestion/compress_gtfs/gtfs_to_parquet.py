import os
import time
import tempfile
from typing import Tuple

import polars as pl
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as pd

from lamp_py.runtime_utils.process_logger import ProcessLogger

from .gtfs_schema_map import gtfs_schema_list
from .gtfs_schema_map import gtfs_schema
from .schedule_details import ScheduleDetails
from .schedule_details import schedules_to_compress


def frame_parquet_diffs(
    new_frame: pl.DataFrame,
    pq_path: str,
    gtfs_table_file: str,
    filter_date: int,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    compare new_frame records to applicable records from an existing parquet file

    creates 3 frames based on diffs:
        - new_records -> records in new_frame that are not found in parquet file
        - same_records -> records that are in new_frame and parquet file
        - old_records -> records in parquet file that are not found in new_frame

    :param new_frame: records to compare to parquet file
    :param pq_path: path to parquet file for comparison
    :param gtfs_table_file: (ie. stop_times.txt)
    :param filter_date: value for inclusive filter on parquet file as YYYYMMDD (ie. service_date)

    :return Tuple[
        old_records: polars.DataFrame,
        same_records: polars.DataFrame,
        new_records: polars.DataFrame,
    ]
    """
    pq_filter = (pc.field("gtfs_active_date") <= filter_date) & (
        pc.field("gtfs_end_date") >= filter_date
    )
    pq_frame = pl.from_arrow(
        pq.ParquetDataset(pq_path, filters=pq_filter).read()
    )

    # mypy type guard...
    if isinstance(pq_frame, pl.Series):
        pq_frame = pq_frame.to_frame()

    join_columns = tuple(gtfs_schema(gtfs_table_file).keys())

    # anti join of new_frame to pq_frame will create frame of only new records not found in pq_frame
    # empty frame created if no new records exist
    new_records = new_frame.join(
        pq_frame.select(join_columns),
        how="anti",
        on=join_columns,
        join_nulls=True,
    ).drop("from_zip")

    # left join to create frame of old and same records
    pq_frame = pq_frame.join(
        new_frame.select(join_columns + ("from_zip",)),
        how="left",
        on=join_columns,
        join_nulls=True,
    )
    same_records = pq_frame.filter(pl.col("from_zip").eq(True)).drop("from_zip")
    old_records = pq_frame.filter(pl.col("from_zip").is_null()).drop("from_zip")

    return old_records, same_records, new_records


def merge_frame_with_parquet(
    merge_frame: pl.DataFrame, export_path: str, filter_date: int
) -> None:
    """
    merge merge_frame with existing parqut file (export_path) and over-write with results

    all parquet read/write operations are done in batches to constrain memory usage

    :param merge_frame: records to merge into export_path parquet file
    :param export_path: existing parquet file to merge with merge_frame
    :param filter_date: value for exclusive filter on parquet files as YYYYMMDD (ie. service_date)
    """
    batch_size = 1024 * 256
    if merge_frame.shape[0] == 0:
        # No records to merge with parquet file
        return

    # sort stop_times and trips frames to reduce file size
    if "/stop_times.parquet" in export_path:
        merge_frame = merge_frame.sort(by=["stop_id", "trip_id"])
    if "/trips.parquet" in export_path:
        merge_frame = merge_frame.sort(by=["route_id", "service_id"])

    pq_schema = merge_frame.to_arrow().schema

    with tempfile.TemporaryDirectory() as temp_dir:
        new_pq_path = os.path.join(temp_dir, "new.parquet")
        filter_pq_path = os.path.join(temp_dir, "filter.parquet")

        merge_frame.write_parquet(
            new_pq_path, use_pyarrow=True, statistics=True
        )

        # create filtered parquet file, excluding records from merge_frame
        pq_filter = (pc.field("gtfs_active_date") > filter_date) | (
            pc.field("gtfs_end_date") < filter_date
        )
        filter_ds = pd.dataset(export_path).filter(pq_filter)
        with pq.ParquetWriter(filter_pq_path, schema=pq_schema) as writer:
            for batch in filter_ds.to_batches(batch_size=batch_size):
                writer.write_batch(batch)

        # over-write export_path file with merged dataset
        combined_ds = pd.dataset((filter_pq_path, new_pq_path))
        with pq.ParquetWriter(export_path, schema=pq_schema) as writer:
            for batch in combined_ds.to_batches(batch_size=batch_size):
                writer.write_batch(batch)


def compress_gtfs_file(
    gtfs_table_file: str, schedule_details: ScheduleDetails
) -> None:
    """
    compress an indivdual gtfs_table_file (ie. stop_times.txt) into yearly parquet
    partitioned parquet file(s)

    yearly partition is based on ScheduleDetals.active_from_int value (1 day after published_dt)

    will perform 1 of 3 operations:
    1.  if a yearly parquet file already exists, perform differential
        merge operation on existing parquet file with gtfs_table_file

    2.  yearly parquet file does not exist, but previous year file does exist:
        - update previous year file with "gtfs_end_date" values going to end of previous year
        - create new yearly partition for current year (with merged records from previous year)

    3.  no parition files exist (current or previous year), create new partition
        file for current year (process initialization)

    :param gtfs_table_file: (ie. stop_times.txt)
    :param schedule_details: data required for schedule compression operation
    """
    partition_year = int(str(schedule_details.active_from_int)[:4])

    gtfs_table = gtfs_table_file.replace(".txt", "")

    export_path = os.path.join(
        schedule_details.tmp_folder,
        f"{partition_year}",
        f"{gtfs_table}.parquet",
    )
    last_export_path = os.path.join(
        schedule_details.tmp_folder,
        f"{partition_year-1}",
        f"{gtfs_table}.parquet",
    )

    new_frame = schedule_details.gtfs_to_frame(gtfs_table_file)

    if os.path.exists(export_path):
        #
        # regular merge operation (with export_path)
        #
        old_records, same_records, new_records = frame_parquet_diffs(
            new_frame=new_frame,
            pq_path=export_path,
            gtfs_table_file=gtfs_table_file,
            filter_date=schedule_details.active_from_int,
        )

        # "gtfs_end_date":
        #   (same or new records) set to schedule_details.active_to_int
        #   (old records) set to schedule_details.published_int (day before active_to_int)
        same_records = same_records.with_columns(
            pl.lit(schedule_details.active_to_int).alias("gtfs_end_date")
        )
        old_records = old_records.with_columns(
            pl.lit(schedule_details.published_int).alias("gtfs_end_date")
        )

        merge_records = pl.concat(
            (old_records, same_records, new_records),
            how="diagonal",
        )
        merge_frame_with_parquet(
            merge_records,
            export_path,
            schedule_details.active_from_int,
        )

    elif os.path.exists(last_export_path):
        #
        # new year merge operation (with last_export_path)
        #
        end_last_year = int(f"{partition_year-1}1231")
        start_current_year = int(f"{partition_year}0101")
        old_records, same_records, new_records = frame_parquet_diffs(
            new_frame=new_frame,
            pq_path=last_export_path,
            gtfs_table_file=gtfs_table_file,
            filter_date=end_last_year,
        )

        # for last year, old and same records applicable TO last day of the previous year
        last_year_records = pl.concat(
            (old_records, same_records),
            how="diagonal",
        ).with_columns(
            pl.lit(end_last_year).alias("gtfs_end_date"),
        )
        merge_frame_with_parquet(
            last_year_records,
            last_export_path,
            end_last_year,
        )

        # for current year, old and same records applicable FROM the start of the year
        # same records applicable TO active_to_int
        # old records applicable TO published_int
        same_records = same_records.with_columns(
            pl.lit(start_current_year).alias("gtfs_active_date"),
            pl.lit(schedule_details.active_to_int).alias("gtfs_end_date"),
        )
        old_records = old_records.with_columns(
            pl.lit(start_current_year).alias("gtfs_active_date"),
            pl.lit(schedule_details.published_int).alias("gtfs_end_date"),
        )
        pl.concat(
            (old_records, same_records, new_records),
            how="diagonal",
        ).filter(
            pl.col("gtfs_end_date") > pl.col("gtfs_active_date")
        ).write_parquet(
            export_path, use_pyarrow=True, statistics=True
        )
    else:
        #
        # no partition file exists (current or last)
        # create new partition file, if new records exist (initialize process)
        #
        if new_frame.shape[0] == 0:
            return

        new_frame.drop("from_zip").write_parquet(
            export_path, use_pyarrow=True, statistics=True
        )


def compress_gtfs_schedule(schedule_details: ScheduleDetails) -> None:
    """
    compress all table files of gtfs schedule into parquet files partitioned by year

    this process is currently configured to run sequentially (oldest -> newest)

    the feed_info table file will be processed last, so that in the case of a
    process failure, re-processsing of schedules will be possible

    :param schedule_details: data required for schedule compression operation
    """
    retry_attemps = 3

    for gtfs_file in gtfs_schema_list():
        logger = ProcessLogger(
            "compress_gtfs_schedule_file",
            gtfs_file=gtfs_file,
        )
        logger.log_start()
        for attempt in range(retry_attemps + 1):
            try:
                logger.add_metadata(retry_attemps=attempt)
                compress_gtfs_file(gtfs_file, schedule_details)
                logger.log_complete()
                break
            except Exception as exception:
                # wait for gremlins to disappear...
                time.sleep(5)
                if attempt == retry_attemps:
                    logger.log_failure(exception)
                    raise exception


def gtfs_to_parquet() -> None:
    """
    run gtfs -> parquet schedule compression process locally and then sync with S3 bucket
    """
    gtfs_tmp_folder = "/tmp/compress-gtfs"
    logger = ProcessLogger(
        "compress_gtfs_schedules", gtfs_tmp_folder=gtfs_tmp_folder
    )
    logger.log_start()

    feed = schedules_to_compress(gtfs_tmp_folder)
    logger.add_metadata(schedule_count=feed.shape[0])

    # compress each schedule in feed
    for schedule in feed.rows(named=True):
        schedule_url = schedule["archive_url"]
        schedule_pub_dt = schedule["published_dt"]
        schedule_details = ScheduleDetails(
            schedule_url,
            schedule_pub_dt,
            gtfs_tmp_folder,
        )
        compress_gtfs_schedule(schedule_details)

    # send updates to S3 bucket...
    for year in set(feed["published_dt"].dt.strftime("%Y").unique()):
        year_path = os.path.join(gtfs_tmp_folder, year)
        for _ in os.listdir(year_path):
            # upload file to S3
            continue

    logger.log_complete()
