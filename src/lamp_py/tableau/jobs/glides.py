from typing import Optional, Type

import dataframely as dy
import polars as pl
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.ingestion.glides import OperatorSignInsTable, TripUpdatesTable
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.remote_files import (
    glides_operator_signed_in,
    glides_trips_updated,
    tableau_glides_all_operator_signed_in,
    tableau_glides_all_trips_updated,
)
from lamp_py.tableau.hyper import HyperJob
from lamp_py.utils.dataframely import has_metadata, with_nullable

GLIDES_TABLEAU_PROJECT = "Glides"

TripUpdatesTableau: Type[dy.Schema] = type(
    "TripUpdatesTableau",
    (dy.Schema,),
    {
        **{
            k: with_nullable(v, True)
            for k, v in TripUpdatesTable.columns().items()
            if not has_metadata(v, "reader_roles")
        },
        **{
            "data.metadata.inputTimestamp": dy.Datetime(nullable=True, time_unit="ms"),
            "data.tripUpdates.previousTripKey.serviceDate": dy.Date(nullable=True),
            "data.tripUpdates.tripKey.serviceDate": dy.Date(nullable=True),
        },
    },
)

OperatorSignInsTableau: Type[dy.Schema] = type(
    "OperatorSignInsTableau",
    (dy.Schema,),
    {
        **{
            k: with_nullable(v, True)
            for k, v in OperatorSignInsTable.columns().items()
            if not has_metadata(v, "reader_roles")
        },
        **{
            "data.metadata.inputTimestamp": dy.Datetime(nullable=True, time_unit="ms"),
            "data.signedInAt": dy.Datetime(nullable=True, time_unit="ms"),
        },
    },
)


def create_trips_updated_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(bucket_name=glides_trips_updated.bucket, file_prefix=glides_trips_updated.prefix)
    ds_paths = [s.replace("s3://", "") for s in s3_uris]

    if num_files is not None:
        ds_paths = ds_paths[-num_files:]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=S3FileSystem(),
    )

    with pq.ParquetWriter(job.local_parquet_path, schema=job.output_processed_schema) as writer:
        for batch in ds.to_batches(batch_size=500_000):
            polars_df = pl.from_arrow(batch)
            if not isinstance(polars_df, pl.DataFrame):
                raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")
            polars_df = polars_df.with_columns(
                pl.col("data.metadata.inputTimestamp")
                .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
                .dt.convert_time_zone(time_zone="US/Eastern")
                .dt.replace_time_zone(None),
                pl.col("time").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
                # all of these service dates and start/end times are already EST - don't do any conversions
                pl.col("data.tripUpdates.previousTripKey.serviceDate").str.to_date("%Y-%m-%d", strict=False),
                # pl.col("data.tripUpdates.previousTripKey.startTime").str.to_time("%H:%M:%S", strict=False),
                # pl.col("data.tripUpdates.previousTripKey.endTime").str.to_time("%H:%M:%S", strict=False),
                pl.col("data.tripUpdates.tripKey.serviceDate").str.to_date("%Y-%m-%d", strict=False),
                # pl.col("data.tripUpdates.tripKey.startTime").str.to_time("%H:%M:%S", strict=False),
                # pl.col("data.tripUpdates.tripKey.endTime").str.to_time("%H:%M:%S", strict=False),
                # pl.col("data.tripUpdates.startTime").str.to_time("%H:%M:%S", strict=False),
                # pl.col("data.tripUpdates.endTime").str.to_time("%H:%M:%S", strict=False),
            )

            writer.write_table(polars_df.select(job.output_processed_schema.names).to_arrow())


def create_operator_signed_in_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(
        bucket_name=glides_operator_signed_in.bucket, file_prefix=glides_operator_signed_in.prefix
    )
    ds_paths = [s.replace("s3://", "") for s in s3_uris]

    if num_files is not None:
        ds_paths = ds_paths[-num_files:]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=S3FileSystem(),
    )

    with pq.ParquetWriter(job.local_parquet_path, schema=job.output_processed_schema) as writer:
        for batch in ds.to_batches(batch_size=500_000):
            polars_df = pl.from_arrow(batch)
            if not isinstance(polars_df, pl.DataFrame):
                raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")
            polars_df = polars_df.with_columns(
                pl.col("data.metadata.inputTimestamp")
                .str.strptime(pl.Datetime(time_unit="ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
                .dt.convert_time_zone(time_zone="US/Eastern")
                .dt.replace_time_zone(None),
                pl.col("data.signedInAt")
                .str.strptime(pl.Datetime(time_unit="ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
                .dt.convert_time_zone(time_zone="US/Eastern")
                .dt.replace_time_zone(None),
                pl.col("time").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            )

            writer.write_table(polars_df.select(job.output_processed_schema.names).to_arrow())


class HyperGlidesTripUpdates(HyperJob):
    """HyperJob for Trip Updates Glides Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_glides_all_trips_updated.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_glides_all_trips_updated.s3_uri,
            lamp_version=tableau_glides_all_trips_updated.version,
            project_name=GLIDES_TABLEAU_PROJECT,
        )

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return TripUpdatesTableau.to_pyarrow_schema()

    def create_parquet(self, _: DatabaseManager | None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        create_trips_updated_glides_parquet(self, None)
        return True


class HyperGlidesOperatorSignIns(HyperJob):
    """HyperJob for Operator Sign-Ins Glides Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_glides_all_operator_signed_in.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_glides_all_operator_signed_in.s3_uri,
            lamp_version=tableau_glides_all_operator_signed_in.version,
            project_name=GLIDES_TABLEAU_PROJECT,
        )

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return OperatorSignInsTableau.to_pyarrow_schema()

    def create_parquet(self, _: DatabaseManager | None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        create_operator_signed_in_glides_parquet(self, None)
        return True
