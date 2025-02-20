from typing import Optional
from datetime import datetime
from datetime import timezone

import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.tableau.hyper import HyperJob
from lamp_py.runtime_utils.remote_files import bus_events
from lamp_py.runtime_utils.remote_files import tableau_bus_all
from lamp_py.runtime_utils.remote_files import tableau_bus_recent
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.aws.s3 import file_list_from_s3_with_details
from lamp_py.aws.s3 import object_exists

bus_schema = pyarrow.schema(
    [
        ("service_date", pyarrow.large_string()),
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
        ("plan_trip_id", pyarrow.large_string()),
        ("exact_plan_trip_match", pyarrow.bool_()),
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

    with pq.ParquetWriter(job.local_parquet_path, schema=job.parquet_schema) as writer:
        for batch in ds.to_batches(batch_size=500_000):
            polars_df = pl.from_arrow(batch)

            if not isinstance(polars_df, pl.DataFrame):
                raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

            # Convert datetime to Eastern Time
            polars_df = polars_df.with_columns(
                pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
                pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
                pl.col("gtfs_travel_to_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            )

            # Convert seconds columns to be aligned with Eastern Time
            polars_df = polars_df.with_columns(
                (pl.col("gtfs_travel_to_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
                .dt.total_seconds()
                .alias("gtfs_travel_to_seconds"),
                (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
                .dt.total_seconds()
                .alias("stop_arrival_seconds"),
                (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
                .dt.total_seconds()
                .alias("stop_departure_seconds"),
            )

            polars_df = polars_df.with_columns(pl.col("service_date").str.strptime(pl.Date, "%Y%m%d", strict=False))

            writer.write_table(polars_df.to_arrow())


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
    def parquet_schema(self) -> pyarrow.schema:
        return bus_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        # only run once per day after 11AM UTC
        if object_exists(tableau_bus_all.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=tableau_bus_all.bucket,
                file_prefix=tableau_bus_all.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        create_bus_parquet(self, None)
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
    def parquet_schema(self) -> pyarrow.schema:
        return bus_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        create_bus_parquet(self, 7)
        return True
