"""Reprocess archived TripUpdates JSONs to springboard for March 3, when the LRTP dataset was renamed to TERMINAL_PREDICTIONS."""

from datetime import date
from queue import Queue

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import (
    ConfigType,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE


def runner(reprocess_date: list[date] = [date(2026, 2, 21)]) -> None:
    """
    Get all of the March 3 TripUpdates filepaths in the delta archive, sort them into
    batches of similar gtfs-rt files, convert each batch into tables, write the
    tables to parquet files in the springboard bucket, add the parquet
    filepaths to the metadata table as unprocessed.
    """
    logger = ProcessLogger(process_name="reingest_files", dates=",".join([str(d) for d in reprocess_date]))
    logger.log_start()

    # list[ConfigType]
    configs: list[ConfigType] = [
        ConfigType.RT_TRIP_UPDATES,
        ConfigType.DEV_GREEN_RT_TRIP_UPDATES,
    ]

    # list all files in the delta archive for the given date(s)
    filepaths: list[str] = []
    for d in reprocess_date:
        filepaths += file_list_from_s3(
            bucket_name=S3_ARCHIVE,
            file_prefix=d.strftime("lamp/delta/%Y/%m/%d/"),
        )

    for config in configs:
        files_to_process: list[str] = [f for f in filepaths if ConfigType.from_filename(f) == config]

        converter = GtfsRtConverter(config, Queue())
        converter.add_files(files_to_process)
        for table in converter.process_files():
            converter.continuous_pq_update(table)

    logger.log_complete()
