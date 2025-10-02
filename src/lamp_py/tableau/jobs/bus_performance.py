from typing import Optional
from datetime import datetime
from datetime import timezone
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions

from lamp_py.runtime_utils.remote_files import bus_events
from lamp_py.runtime_utils.remote_files import tableau_bus_all
from lamp_py.runtime_utils.remote_files import tableau_bus_recent
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.aws.s3 import file_list_from_s3_with_details
from lamp_py.aws.s3 import object_exists

# temporary - ticket in backlog to implement this split as per-rating instead
BUS_ALL_NDAYS = 365
BUS_RECENT_NDAYS = 7
# this schema and the order of this schema SHOULD match what comes out
# of the polars version from bus_performance_manager.
# see select() comment below..
bus_schema = pyarrow.schema(
    [
        ("service_date", pyarrow.date32()),  # change to date type
        ("route_id", pyarrow.large_string()),
        ("trip_id", pyarrow.large_string()),
        ("start_time", pyarrow.int64()),
        ("start_dt", pyarrow.timestamp("us")),
        ("stop_count", pyarrow.uint32()),
        ("direction_id", pyarrow.int8()),
        ("stop_id", pyarrow.large_string()),
        ("stop_sequence", pyarrow.int64()),
        ("vehicle_id", pyarrow.large_string()),
        ("vehicle_label", pyarrow.large_string()),
        ("gtfs_travel_to_dt", pyarrow.timestamp("us")),
        ("tm_stop_sequence", pyarrow.int64()),
        ("tm_scheduled_time_dt", pyarrow.timestamp("us")),
        ("tm_actual_arrival_dt", pyarrow.timestamp("us")),
        ("tm_actual_departure_dt", pyarrow.timestamp("us")),
        ("tm_scheduled_time_sam", pyarrow.int64()),
        ("tm_actual_arrival_time_sam", pyarrow.int64()),
        ("tm_actual_departure_time_sam", pyarrow.int64()),
        # ("plan_trip_id", pyarrow.large_string()),
        # ("exact_plan_trip_match", pyarrow.bool_()),
        ("block_id", pyarrow.large_string()),
        ("service_id", pyarrow.large_string()),
        ("route_pattern_id", pyarrow.large_string()),
        ("route_pattern_typicality", pyarrow.int64()),
        ("direction", pyarrow.large_string()),
        ("direction_destination", pyarrow.large_string()),
        ("plan_stop_count", pyarrow.uint32()),
        ("plan_start_time", pyarrow.int64()),
        ("plan_start_dt", pyarrow.timestamp("us")),
        ("stop_name", pyarrow.large_string()),
        ("plan_stop_departure_dt", pyarrow.timestamp("us")),
        ("plan_travel_time_seconds", pyarrow.int64()),
        ("plan_route_direction_headway_seconds", pyarrow.int64()),
        ("plan_direction_destination_headway_seconds", pyarrow.int64()),
        ("stop_arrival_dt", pyarrow.timestamp("us")),
        ("stop_departure_dt", pyarrow.timestamp("us")),
        ("gtfs_travel_to_seconds", pyarrow.int64()),
        ("stop_arrival_seconds", pyarrow.int64()),
        ("stop_departure_seconds", pyarrow.int64()),
        ("travel_time_seconds", pyarrow.int64()),
        ("dwell_time_seconds", pyarrow.int64()),
        ("route_direction_headway_seconds", pyarrow.int64()),
        ("direction_destination_headway_seconds", pyarrow.int64()),
        ("gtfs_sort_dt", pyarrow.timestamp("us")),
        ("gtfs_departure_dt", pyarrow.timestamp("us")),
        ("gtfs_arrival_dt", pyarrow.timestamp("us")),
    ]
)


def create_bus_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Join bus_events files into single parquet file for upload to Tableau
    """
    s3_uris = file_list_from_s3(bucket_name=bus_events.bucket, file_prefix=bus_events.prefix)
    ds_paths = [s.replace("s3://", "") for s in s3_uris]

    if num_files is not None:
        ds_paths = ds_paths[-num_files:]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=S3FileSystem(),
    )

    with pq.ParquetWriter(job.local_parquet_path, schema=job.output_processed_schema) as writer:

        for batch in ds.to_batches(batch_size=500_000, batch_readahead=1, fragment_readahead=0):
            # this select() is here to make sure the order of the polars_df
            # schema is the same as the bus_schema above.
            # order of schema matters to the ParquetWriter

            # if the bus_schema above is in the same order as the batch
            # schema, then the select will do nothing - as expected

            polars_df = pl.from_arrow(batch).select(job.output_processed_schema.names)

            if not isinstance(polars_df, pl.DataFrame):
                raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

            writer.write_table(apply_bus_analysis_conversions(polars_df))


class HyperBusPerformanceAll(HyperJob):
    """HyperJob for ALL LAMP RT Bus Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_bus_all.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_bus_all.s3_uri,
            lamp_version=tableau_bus_all.version,
        )

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return bus_schema

    def create_parquet(self, _: DatabaseManager | None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        # only run once per day after 11AM UTC
        if object_exists(tableau_bus_all.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=tableau_bus_all.bucket,
                file_prefix=tableau_bus_all.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        create_bus_parquet(self, BUS_ALL_NDAYS)
        return True


class HyperBusPerformanceRecent(HyperJob):
    """HyperJob for RECENT LAMP RT Bus Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_bus_recent.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_bus_recent.s3_uri,
            lamp_version=tableau_bus_recent.version,
        )

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return bus_schema

    def create_parquet(self, _: DatabaseManager | None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        create_bus_parquet(self, BUS_RECENT_NDAYS)
        return True
