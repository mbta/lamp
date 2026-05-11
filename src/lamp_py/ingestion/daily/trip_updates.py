from datetime import date, datetime, timezone
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.daily.config import END_HOUR, START_HOUR
import polars as pl
import os
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3Location
from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter
from lamp_py.ingestion.backfill.delta_reingestion import delta_reingestion_runner
from lamp_py.ingestion.backfill.convert_gtfs_rt_fullset import GtfsRtFullPartitionConverter
from queue import Queue


def within_daily_processing_window() -> bool:
    """
    Check if current time is within the daily processing window defined by START_HOUR and END_HOUR
    """
    now = datetime.now(timezone.utc)
    hour = now.hour
    return START_HOUR <= hour < END_HOUR


def reprocess_trip_updates_terminal_prediction() -> bool:
    """
    Filter down fullset trip updates to just terminal predictions for heavy and light rail, and re-upload to a different location in s3 for use in the terminal prediction model training.
    """
    all_terminal_stops = LightRailFilter.terminal_stop_ids + HeavyRailFilter.terminal_stop_ids
    polars_filter = pl.col("trip_update.trip.route_id").is_in(
        ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
    ) & pl.col("trip_update.stop_time_update.stop_id").is_in(all_terminal_stops)

    pass


# def consolidate_partitions_for_archive(local_converter_partition_path: date) -> bool:

#     write_dataset_to_single_parquet_partitioned_and_sorted(
#         local_converter_partition_path,
#         local_combined_file,
#         partition_column=converter.partition_column(),
#         in_partition_sort=converter.table_sort_order(),
#         debug_flag=True,
#     )

#     #### Stage 3: local to remote (one to one)

#     # upload local to remote
#     upload_file(
#         local_combined_file,
#         s3_combined_file,
#     )


def reprocess_trip_updates(start_date: date, end_date: date) -> bool:
    """
    Full encapsulated method to call all of this backfill job
    """

    local_tmp_output = "/tmp/gtfs-rt-continuous/"

    if not os.path.exists(local_tmp_output):
        os.makedirs(local_tmp_output)

    final_output_path = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_FULLSET")
    final_output_path_daily = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES")

    # construct and run converter once per day
    converter = GtfsRtFullPartitionConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=Queue(),
        local_output_location=local_tmp_output,
        # remote_output_location=final_output_path_daily,
        max_workers=8,
        verbose=True,
        time_chunk_minutes=15,
    )

    delta_reingestion_runner(
        start_date=start_date,
        end_date=end_date,
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        converter=converter,
        in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
        bucket=S3_ARCHIVE,
    )

    return True
