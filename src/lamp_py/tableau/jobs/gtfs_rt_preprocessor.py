import os
from typing import Optional
from datetime import datetime
from datetime import timezone
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.tableau.hyper import HyperJob

from lamp_py.runtime_utils.remote_files import S3Location

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.aws.s3 import file_list_from_s3_with_details
from lamp_py.aws.s3 import object_exists
class FilteredHyperJob(HyperJob):
    """HyperJob for Generic GTFS RT conversion - Converting from OOP to Composition"""

    def __init__(
        self,
        remote_input_location: S3Location,
        remote_output_location: S3Location,
        processed_schema: pyarrow.schema,
        bucket_filter: str = None,
        object_filter: str = None,
        parquet_filter: function = None,
        dataframe_filter: function = None,
    ) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=remote_output_location.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=remote_output_location.s3_uri,
            lamp_version=remote_output_location.version,
        )
        self.remote_input_location = remote_input_location
        self.remote_output_location = remote_output_location
        self.processed_schema = processed_schema
        self.bucket_filter = bucket_filter
        self.object_filter = object_filter
        self.parquet_filter = parquet_filter
        self.dataframe_filter = dataframe_filter

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return self.processed_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        # only run once per day after 11AM UTC
        if object_exists(self.remote_input_location.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=self.remote_input_location.bucket,
                file_prefix=self.remote_input_location.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        self.create_tableau_parquet(num_files=10)
        return True

    def create_tableau_parquet(self, num_files: Optional[int]) -> None:
        """
        Join files into single parquet file for upload to Tableau. apply filter and conversions as necessary
        """
        file_prefix = os.path.join(self.remote_input_location.prefix, self.tableau_bucket_filter)
        s3_uris = file_list_from_s3(
            bucket_name=self.remote_input_location.bucket, file_prefix=file_prefix, in_filter=self.tableau_object_filter
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        if num_files is not None:
            ds_paths = ds_paths[-num_files:]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        with pq.ParquetWriter(self.local_parquet_path, schema=self.parquet_schema) as writer:
            for batch in ds.to_batches(
                batch_size=500_000, columns=self.tableau_schema.names, filter=self.tableau_parquet_filter
            ):

                polars_df = pl.from_arrow(batch)
                if not isinstance(polars_df, pl.DataFrame):
                    raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")
                writer.write_table(self.tableau_dataframe_filter(polars_df))
