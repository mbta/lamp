from typing import Optional
from datetime import datetime
from datetime import timezone

import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.tableau.hyper import HyperJob
from lamp_py.runtime_utils.remote_files import (
    glides_trip_updates,
    glides_operator_sign_ins,
    tableau_glides_trip_updates,
    tableau_glides_operator_sign_ins,
)
from lamp_py.ingestion.glides import TripUpdates, OperatorSignIns
from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_with_details, object_exists


# these are re-defined here for clarity. These will be asserted against
glides_trip_updates_schema = pyarrow.schema(
    [
        ("data.metadata.location.gtfsId", pyarrow.large_string()),
        ("data.metadata.location.todsId", pyarrow.large_string()),
        ("data.metadata.author.emailAddress", pyarrow.large_string()),
        ("data.metadata.author.badgeNumber", pyarrow.large_string()),
        ("data.metadata.inputType", pyarrow.large_string()),
        ("data.metadata.inputTimestamp", pyarrow.large_string()), # probably this one
        ("id", pyarrow.large_string()),
        ("type", pyarrow.large_string()), # probably this one
        ("time", pyarrow.timestamp("ms")), # probably this one
        ("source", pyarrow.large_string()),
        ("specversion", pyarrow.large_string()),
        ("dataschema", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.serviceDate", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.previousTripKey.tripId", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.startLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.startLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.endLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.endLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.startTime", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.previousTripKey.endTime", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.previousTripKey.revenue", pyarrow.large_string()),
        ("data.tripUpdates.previousTripKey.glidesId", pyarrow.large_string()),
        ("data.tripUpdates.type", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.serviceDate", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.tripKey.tripId", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.startLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.startLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.endLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.endLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.startTime", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.tripKey.endTime", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.revenue", pyarrow.large_string()),
        ("data.tripUpdates.tripKey.glidesId", pyarrow.large_string()),
        ("data.tripUpdates.comment", pyarrow.large_string()),
        ("data.tripUpdates.startLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.startLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.endLocation.gtfsId", pyarrow.large_string()),
        ("data.tripUpdates.endLocation.todsId", pyarrow.large_string()),
        ("data.tripUpdates.startTime", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.endTime", pyarrow.large_string()), # probably this one
        ("data.tripUpdates.cars", pyarrow.large_string()),
        ("data.tripUpdates.revenue", pyarrow.large_string()),
        ("data.tripUpdates.dropped", pyarrow.large_string()),
        ("data.tripUpdates.scheduled", pyarrow.large_string()),
    ]
)

glides_operator_sign_ins_schema = pyarrow.schema(
    [
        ("data.metadata.location.gtfsId", pyarrow.large_string()),
        ("data.metadata.location.todsId", pyarrow.large_string()),
        ("data.metadata.author.emailAddress", pyarrow.large_string()),
        ("data.metadata.author.badgeNumber", pyarrow.large_string()),
        ("data.metadata.inputType", pyarrow.large_string()),
        ("data.metadata.inputTimestamp", pyarrow.large_string()), # probably this one
        ("data.operator.badgeNumber", pyarrow.large_string()),
        ("data.signedInAt", pyarrow.large_string()), # probably this one
        ("data.signature.type", pyarrow.large_string()),
        ("data.signature.version", pyarrow.int16()),
        ("id", pyarrow.large_string()),
        ("type", pyarrow.large_string()),
        ("time", pyarrow.timestamp("ms")),
        ("source", pyarrow.large_string()),
        ("specversion", pyarrow.large_string()),
        ("dataschema", pyarrow.large_string()),
    ]
)


def create_trip_updates_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(bucket_name=glides_trip_updates.bucket, file_prefix=glides_trip_updates.prefix)
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

            # # Convert datetime to Eastern Time
            # polars_df = polars_df.with_columns(
            #     pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            #     pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            #     pl.col("gtfs_travel_to_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            # )

            # # Convert seconds columns to be aligned with Eastern Time
            # polars_df = polars_df.with_columns(
            #     (pl.col("gtfs_travel_to_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("gtfs_travel_to_seconds"),
            #     (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("stop_arrival_seconds"),
            #     (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("stop_departure_seconds"),
            # )

            # polars_df = polars_df.with_columns(pl.col("service_date").str.strptime(pl.Date, "%Y%m%d", strict=False))

            writer.write_table(polars_df.to_arrow())


def create_operator_signins_glides_parquet(job: HyperJob, num_files: Optional[int]) -> None:
    """
    Grab the glides_events files and process
    """
    s3_uris = file_list_from_s3(bucket_name=glides_operator_sign_ins.bucket, file_prefix=glides_operator_sign_ins.prefix)
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

            # # Convert datetime to Eastern Time
            # polars_df = polars_df.with_columns(
            #     pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            #     pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            #     pl.col("gtfs_travel_to_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
            # )

            # # Convert seconds columns to be aligned with Eastern Time
            # polars_df = polars_df.with_columns(
            #     (pl.col("gtfs_travel_to_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("gtfs_travel_to_seconds"),
            #     (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("stop_arrival_seconds"),
            #     (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            #     .dt.total_seconds()
            #     .alias("stop_departure_seconds"),
            # )

            # polars_df = polars_df.with_columns(pl.col("service_date").str.strptime(pl.Date, "%Y%m%d", strict=False))

            writer.write_table(polars_df.to_arrow())


class HyperGlidesTripUpdates(HyperJob):
    """HyperJob for Trip Updates Glides Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_glides_trip_updates.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_glides_trip_updates.s3_uri,
            lamp_version=tableau_glides_trip_updates.version,
        )

        tu = TripUpdates()
        assert glides_trip_updates_schema == tu.event_schema

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return glides_trip_updates_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        # only run once per day after 11AM UTC - 6 or 7 AM EST
        if object_exists(tableau_glides_trip_updates.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=tableau_glides_trip_updates.bucket,
                file_prefix=tableau_glides_trip_updates.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        create_trip_updates_glides_parquet(self, None)
        return True


class HyperGlidesOperatorSignIns(HyperJob):
    """HyperJob for Operator Sign-Ins Glides Data"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=tableau_glides_operator_sign_ins.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=tableau_glides_operator_sign_ins.s3_uri,
            lamp_version=tableau_glides_operator_sign_ins.version,
        )

        osi = OperatorSignIns()
        assert glides_operator_sign_ins_schema == osi.event_schema

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return glides_operator_sign_ins_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        # only run once per day after 11AM UTC - 6 or 7 AM EST
        if object_exists(tableau_glides_operator_sign_ins.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=tableau_glides_operator_sign_ins.bucket,
                file_prefix=tableau_glides_operator_sign_ins.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        create_operator_signins_glides_parquet(self, None)
        return True
