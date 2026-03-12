"""Reprocess archived TripUpdates JSONs to springboard for March 3, when the LRTP dataset was renamed to TERMINAL_PREDICTIONS."""

from datetime import date


from lamp_py.aws.s3 import get_s3_client
from lamp_py.aws.s3 import delete_object, file_list_from_s3, object_exists
from lamp_py.ingestion.converter import (
    ConfigType,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3_SPRINGBOARD, S3_INCOMING


def runner(reprocess_date: list[date] = [date(2026, 2, 27)]) -> None:
    """
    Get all of the March 3 TripUpdates filepaths in the delta archive, sort them into
    batches of similar gtfs-rt files, convert each batch into tables, write the
    tables to parquet files in the springboard bucket, add the parquet
    filepaths to the metadata table as unprocessed.
    """
    logger = ProcessLogger(process_name="reingest_files", dates=",".join([str(d) for d in reprocess_date]))
    logger.log_start()

    configs: dict[ConfigType, list[str]] = {
        ConfigType.RT_TRIP_UPDATES: ["RT_TRIP_UPDATES", "TERMINAL_PREDICTIONS_TRIP_UPDATES"],
        ConfigType.DEV_GREEN_RT_TRIP_UPDATES: [
            "DEV_GREEN_RT_TRIP_UPDATES",
            "DEV_GREEN_TERMINAL_PREDICTIONS_TRIP_UPDATES",
        ],
    }

    # list all files in the delta archive for the given date(s)
    filepaths: list[str] = []
    for d in reprocess_date:
        filepaths += file_list_from_s3(
            bucket_name=S3_ARCHIVE,
            file_prefix=d.strftime("lamp/delta/%Y/%m/%d/"),
        )

    for config in configs.keys():
        files_to_process: list[str] = [f for f in filepaths if ConfigType.from_filename(f) == config]

        if len(files_to_process) > 0:
            for directory in configs[config]:
                for d in reprocess_date:
                    object_key = f"{S3_SPRINGBOARD}/lamp/{directory}/{d.strftime('year=%Y/month=%-m/day=%-d/%Y-%m-%d')}T00:00:00.parquet"
                    if object_exists(object_key):
                        delete_object(object_key)
                        print(f"Deleted existing object at {object_key} to prepare for reprocessing")
            s3 = get_s3_client()

            for file in files_to_process:
                s3.copy_object(
                    Bucket=S3_INCOMING,
                    CopySource=file.replace("s3://", ""),
                    Key=file.replace(f"s3://{S3_ARCHIVE}/", ""),
                )

    logger.log_complete()
