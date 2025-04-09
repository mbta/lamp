import os
from typing import Optional, Callable
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


# pylint: disable=R0913,R0902
# pylint too many local variables (more than 15)
class FilteredHyperJob(HyperJob):
    """HyperJob for Generic GTFS RT conversion - Converting from OOP to Composition"""

    def __init__(
        self,
        remote_input_location: S3Location,
        remote_output_location: S3Location,
        processed_schema: pyarrow.schema,
        tableau_project_name: str,
        rollup_num_days: int = 7, # default this to a week of data
        bucket_filter: str | None = None,
        object_filter: str | None = None,
        parquet_filter: pc.Expression | None = None,
        dataframe_filter: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name=remote_output_location.prefix.rsplit("/")[-1].replace(".parquet", ".hyper"),
            remote_parquet_path=remote_output_location.s3_uri,
            lamp_version=remote_output_location.version,
            project_name=tableau_project_name,
        )
        self.remote_input_location = remote_input_location
        self.remote_output_location = remote_output_location
        self.processed_schema = processed_schema
        self.rollup_num_days = rollup_num_days
        self.bucket_filter = bucket_filter  # level 0 | by day
        self.object_filter = object_filter  # level 1 | by type
        self.parquet_filter = parquet_filter  # level 2 | by column and simple filter
        self.dataframe_filter = dataframe_filter  # level 3 | complex filter

        # this flag is entirely a developer nice to have. This will ensure that 
        # the hyper job will run immediately when the tableau job is called, and will
        # process and upload a hyper file now, rather than on the hour or after 7am
        # this relies on the FilteredHyperJob persisting across runs - currently it is 
        # constructed on library load, but if it is reconstructed on each run_hyper() invocation, 
        # this will no longer hold. 
        self.first_run = True

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return self.processed_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:

        # this flag is entirely a developer nice to have. This will ensure that 
        # the hyper job will run immediately when the tableau job is called, and will
        # process and upload a hyper file now, rather than on the hour or after 7am   
        # this relies on the FilteredHyperJob persisting across runs - currently it is 
        # constructed on library load, but if it is reconstructed on each run_hyper() invocation, 
        # this will no longer hold.      
        if self.first_run:
            self.create_tableau_parquet(num_files=self.rollup_num_days)
            self.first_run = False

        # only run once per day after 11AM UTC
        if object_exists(self.remote_input_location.s3_uri):
            now_utc = datetime.now(tz=timezone.utc)
            last_mod: datetime = file_list_from_s3_with_details(
                bucket_name=self.remote_input_location.bucket,
                file_prefix=self.remote_input_location.prefix,
            )[0]["last_modified"]

            if now_utc.day == last_mod.day or now_utc.hour < 11:
                return False

        self.create_tableau_parquet(num_files=self.rollup_num_days)
        return True

    def create_tableau_parquet(self, num_files: Optional[int]) -> None:
        """
        Join files into single parquet file for upload to Tableau. apply filter and conversions as necessary
        """
        if self.bucket_filter is not None:
            file_prefix = os.path.join(self.remote_input_location.prefix, self.bucket_filter)
        else:
            file_prefix = self.remote_input_location.prefix

        s3_uris = file_list_from_s3(
            bucket_name=self.remote_input_location.bucket, file_prefix=file_prefix, in_filter=self.object_filter
        )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]
        if num_files is not None:
            ds_paths = ds_paths[-num_files:]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )

        with pq.ParquetWriter(self.local_parquet_path, schema=self.processed_schema) as writer:
            for batch in ds.to_batches(
                batch_size=500_000, columns=self.processed_schema.names, filter=self.parquet_filter
            ):
                
                if self.dataframe_filter is not None:
                    # apply transformations if function passed in
                
                    polars_df = pl.from_arrow(batch)
                    if not isinstance(polars_df, pl.DataFrame):
                        raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

                    polars_df = self.dataframe_filter(polars_df)
                    # filtered on columns of interest and dataframe_filter
                    writer.write_table(polars_df.to_arrow())
                else:    
                    # just write the batch out - filtered on columns of interest
                    writer.write_table(batch)
                  
