#!/usr/bin/env python
from datetime import datetime, timedelta
from typing import List
from lamp_py.bus_performance_manager.event_files import event_files_to_load
from lamp_py.bus_performance_manager.events_metrics import run_bus_performance_pipeline
from lamp_py.bus_performance_manager.events_tm import generate_tm_events
from lamp_py.bus_performance_manager.write_events import write_bus_metrics
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD, bus_events
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
from pyarrow.fs import S3FileSystem
from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_date_range
import polars as pl

from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions

########################################################################
# NOTE: ensure .env PUBLIC_ARCHIVE_BUCKET is pointed to the right bucket
########################################################################

# stop_arrival_dt
# stop_departure_dt


#     # take the later of the two possible arrival times as the true arrival time
#     (
#         pl.when(pl.col("tm_actual_arrival_dt") > pl.col("gtfs_travel_to_dt"))
#         .then(pl.col("tm_actual_arrival_dt"))
#         .otherwise(pl.col("gtfs_arrival_dt"))
#     ).alias("stop_arrival_dt"),
# )
# # take the later of the two possible departure times as the true departure time
# .with_columns(
#     pl.when(pl.col("tm_actual_departure_dt") >= pl.col("stop_arrival_dt"))
#     .then(pl.col("tm_actual_departure_dt"))
#     .otherwise(pl.col("gtfs_departure_dt"))
#     .alias("stop_departure_dt")
# )

# AssertionError
# a = event_files_to_load()

# ##################
# # this is a good runner - 6/17/25
# end_date = datetime(year=2025, month=6, day=17)
# start_date = end_date - timedelta(days=7)
# write_bus_metrics(start_date, end_date, write_local_only=True)
# ##################


# :param service_date: date of service being processed
# :param gtfs_files: list of RT_VEHCILE_POSITION parquet file paths, from S3, that cover service date
# :param tm_files: list of TM/STOP_CROSSING parquet file paths, from S3, that cover service date
date = datetime(year=2025, month=6, day=12)

date_end = date + timedelta(days=1)
bucket_name = "mbta-ctd-dataplatform-springboard"
file_prefix = "lamp/RT_VEHICLE_POSITIONS/"

file_prefix2 = "lamp/TM/STOP_CROSSING"
tm_template = "1{yy}{mm:02}{dd:02}"
springboard_template = "year={yy}/month={mm}/day={dd}/"

# s3://mbta-ctd-dataplatform-springboard/lamp/TM/STOP_CROSSING/120250505.parquet
vp = file_list_from_s3_date_range(
    bucket_name=bucket_name,
    file_prefix=file_prefix,
    path_template=springboard_template,
    end_date=date_end,
    start_date=date,
)
tm = file_list_from_s3_date_range(
    bucket_name=bucket_name, file_prefix=file_prefix2, path_template=tm_template, end_date=date_end, start_date=date
)

df = run_bus_performance_pipeline(service_date=date, gtfs_files=vp, tm_files=tm)

print(df.height)

# generate_tm_events(tm_files=['s3://mbta-ctd-dataplatform-staging-springboard/lamp/TM/STOP_CROSSING/120250502.parquet'])

# tm_stop_crossings['tm_actual_arrival_dt'].is_null().any()
# False
# tm_stop_crossings['tm_actual_departure_dt'].is_null().any()
# False

# # stop_arrival_seconds
# # stop_departure_seconds

# # (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
# # .dt.total_seconds()
# # .alias("stop_arrival_seconds"),
# # (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
# # .dt.total_seconds()
# # .alias("stop_departure_seconds"),

# # this schema and the order of this schema SHOULD match what comes out
# # of the polars version out of bus_performance_manager.
# bus_schema = pyarrow.schema(
#     [
#         ("service_date", pyarrow.date32()),  # change to date type
#         ("route_id", pyarrow.large_string()),
#         ("trip_id", pyarrow.large_string()),
#         ("start_time", pyarrow.int64()),
#         ("start_dt", pyarrow.timestamp("us")),
#         ("stop_count", pyarrow.uint32()),
#         ("direction_id", pyarrow.int8()),
#         ("stop_id", pyarrow.large_string()),
#         ("stop_sequence", pyarrow.int64()),
#         ("vehicle_id", pyarrow.large_string()),
#         ("vehicle_label", pyarrow.large_string()),
#         ("gtfs_travel_to_dt", pyarrow.timestamp("us")),
#         ("tm_stop_sequence", pyarrow.int64()),
#         ("tm_scheduled_time_dt", pyarrow.timestamp("us")),
#         ("tm_actual_arrival_dt", pyarrow.timestamp("us")),
#         ("tm_actual_departure_dt", pyarrow.timestamp("us")),
#         ("tm_scheduled_time_sam", pyarrow.int64()),
#         ("tm_actual_arrival_time_sam", pyarrow.int64()),
#         ("tm_actual_departure_time_sam", pyarrow.int64()),
#         ("plan_trip_id", pyarrow.large_string()),
#         ("exact_plan_trip_match", pyarrow.bool_()),
#         ("block_id", pyarrow.large_string()),
#         ("service_id", pyarrow.large_string()),
#         ("route_pattern_id", pyarrow.large_string()),
#         ("route_pattern_typicality", pyarrow.int64()),
#         ("direction", pyarrow.large_string()),
#         ("direction_destination", pyarrow.large_string()),
#         ("plan_stop_count", pyarrow.uint32()),
#         ("plan_start_time", pyarrow.int64()),
#         ("plan_start_dt", pyarrow.timestamp("us")),
#         ("stop_name", pyarrow.large_string()),
#         ("plan_travel_time_seconds", pyarrow.int64()),
#         ("plan_route_direction_headway_seconds", pyarrow.int64()),
#         ("plan_direction_destination_headway_seconds", pyarrow.int64()),
#         ("stop_arrival_dt", pyarrow.timestamp("us")),
#         ("stop_departure_dt", pyarrow.timestamp("us")),
#         ("gtfs_travel_to_seconds", pyarrow.int64()),
#         ("stop_arrival_seconds", pyarrow.int64()),
#         ("stop_departure_seconds", pyarrow.int64()),
#         ("travel_time_seconds", pyarrow.int64()),
#         ("dwell_time_seconds", pyarrow.int64()),
#         ("route_direction_headway_seconds", pyarrow.int64()),
#         ("direction_destination_headway_seconds", pyarrow.int64()),
#     ]
# )
# s3_uris = file_list_from_s3(bucket_name=bus_events.bucket, file_prefix=bus_events.prefix)
# ds_paths = [s.replace("s3://", "") for s in s3_uris]

# ds_paths = ds_paths[-5:]

# ds = pd.dataset(
#     ds_paths,
#     format="parquet",
#     filesystem=S3FileSystem(),
# )

# with pq.ParquetWriter("test.parquet", schema=bus_schema) as writer:
#     for batch in ds.to_batches(batch_size=500_000):
#         try:
#             # this select() is here to make sure the order of the polars_df
#             # schema is the same as the bus_schema above.
#             # order of schema matters to the ParquetWriter

#             # if the bus_schema above is in the same order as the batch
#             # schema, then the select will do nothing - as expected
#             polars_df = pl.from_arrow(batch).select(bus_schema.names)  # type: ignore[union-attr]

#             if not isinstance(polars_df, pl.DataFrame):
#                 raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

#             writer.write_table(apply_bus_analysis_conversions(polars_df))
#         except Exception as exception:
#             print(exception)
