from datetime import datetime, timezone
from lamp_py.ingestion.daily.config import END_HOUR, START_HOUR
import polars as pl
from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter


def within_daily_processing_window() -> bool:
    """Check if current time is within the daily processing window."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    return START_HOUR <= hour < END_HOUR


def reprocess_trip_updates_terminal_prediction() -> bool:
    """Filter fullset trip updates for heavy/light rail terminal predictions."""
    # Stub for future implementation
    all_terminal_stops = LightRailFilter.terminal_stop_ids + HeavyRailFilter.terminal_stop_ids
    _polars_filter = pl.col("trip_update.trip.route_id").is_in(
        ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
    ) & pl.col("trip_update.stop_time_update.stop_id").is_in(all_terminal_stops)

    return False


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


#         ## Stage 2: local to local (many to 1)

#         # Define the path to your input Parquet files (can use a glob pattern)
#         converter_output_path = f"{local_output_location}/lamp/RT_TRIP_UPDATES/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/"
#         consolidated_parquet_output_file = (
#             f"{local_output_location}/{cur_date.year}_{cur_date.month}_{cur_date.day}.parquet"
#         )

#         # Create a dataset from the input files
#         ds = pd.dataset(converter_output_path, format="parquet")

#         with pq.ParquetWriter(
#             consolidated_parquet_output_file, schema=ds.schema, compression="zstd", compression_level=3
#         ) as writer:
#             for batch in ds.to_batches(batch_size=512 * 1024):
#                 writer.write_batch(batch)

#         #### Stage 3: local to remote (one to one)

#         # upload local to remote
#         upload_file(
#             consolidated_parquet_output_file,
#             consolidated_parquet_output_file.replace(
#                 f"{local_output_location}/",
#                 f"{final_output_path.s3_uri}/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/",
#             ),
#         )


# def reprocess_delta_backfill(config: ConfigType, start_date: date, end_date: date) -> bool:

#     return True
