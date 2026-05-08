from typing import Optional, Type

import dataframely as dy
import polars as pl
import pyarrow

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.ingestion.glides import GlidesRecord, OperatorSignInsRecord, TripUpdatesRecord
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.remote_files import (
    glides_operator_signed_in,
    glides_trips_updated,
    tableau_glides_all_operator_signed_in,
    tableau_glides_all_trips_updated,
)
from lamp_py.tableau.hyper import HyperJob
from lamp_py.utils.dataframely import unnest_columns

GLIDES_TABLEAU_PROJECT = "Glides"

TripUpdatesTableau: Type[GlidesRecord] = type(
    "TripUpdatesTable",
    (GlidesRecord,),
    unnest_columns({"data": TripUpdatesRecord.data})
    | {
        "data.metadata.inputTimestamp": dy.Datetime(time_unit="ms", nullable=True),
        "data.tripUpdates.previousTripKey.serviceDate": dy.Date(nullable=True),
        "data.tripUpdates.tripKey.serviceDate": dy.Date(nullable=True),
    },
)

OperatorSignInsTableau = type(
    "OperatorSignInsTable",
    (GlidesRecord,),
    unnest_columns({"data": OperatorSignInsRecord.data})
    | {
        "data.metadata.inputTimestamp": dy.Datetime(time_unit="ms", nullable=True),
        "data.signedInAt": dy.Datetime(time_unit="ms", nullable=True),
        "time": dy.Datetime(time_unit="ms", nullable=True),
    },
)


def create_trips_updated_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(bucket_name=glides_trips_updated.bucket, file_prefix=glides_trips_updated.prefix)

    if num_files is not None:
        s3_uris = s3_uris[-num_files:]

    ds = (
        pl.scan_parquet(s3_uris)
        .with_columns(
            pl.col("data.metadata.inputTimestamp")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.convert_time_zone(time_zone="US/Eastern")
            .dt.replace_time_zone(None),
            pl.col("time").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            pl.col("data.tripUpdates.previousTripKey.serviceDate").str.to_date("%Y-%m-%d", strict=False),
            pl.col("data.tripUpdates.tripKey.serviceDate").str.to_date("%Y-%m-%d", strict=False),
        )
        .select(TripUpdatesTableau.column_names())
    )

    TripUpdatesTableau.validate(ds, cast=True).write_parquet(job.local_parquet_path)


def create_operator_signed_in_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(
        bucket_name=glides_operator_signed_in.bucket, file_prefix=glides_operator_signed_in.prefix
    )

    if num_files is not None:
        s3_uris = s3_uris[-num_files:]

    ds = (
        pl.scan_parquet(s3_uris)
        .with_columns(
            pl.col("data.metadata.inputTimestamp")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.convert_time_zone(time_zone="US/Eastern")
            .dt.replace_time_zone(None),
            pl.col("data.signedInAt")
            .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
            .dt.convert_time_zone(time_zone="US/Eastern")
            .dt.replace_time_zone(None),
            pl.col("time").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
        )
        .select(OperatorSignInsTableau.column_names())
    )

    OperatorSignInsTableau.validate(ds, cast=True).write_parquet(job.local_parquet_path)


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
