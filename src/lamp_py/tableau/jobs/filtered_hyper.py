from typing import Optional, Callable
from datetime import datetime, timedelta
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.tableau.hyper import HyperJob

from lamp_py.runtime_utils.remote_files import S3Location

from lamp_py.aws.s3 import file_list_from_s3_date_range


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
        rollup_num_days: int = 7,  # default this to a week of data
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
        self.processed_schema = processed_schema  # expected output schema passed in
        self.rollup_num_days = rollup_num_days
        self.parquet_filter = parquet_filter  # level 2 | by column and simple filter
        self.dataframe_filter = dataframe_filter  # level 3 | complex filter

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return self.processed_schema

    def create_parquet(self, _: None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: None) -> bool:
        return self.create_tableau_parquet(num_days=self.rollup_num_days)

    # pylint: disable=R0914
    # pylint too many local variables (more than 15)
    def create_tableau_parquet(self, num_days: Optional[int]) -> bool:
        """
        Join files into single parquet file for upload to Tableau. apply filter and conversions as necessary

        Parameters
        ----------
        num_days : Number of days to query and concatenate. If None, processes all days available

        Returns
        -------
        True if parquet created, False otherwise
        """

        # limitation of filtered hyper only does whole days.
        # update to allow start/end set by hour, to get the entire
        # previous days uploads working
        # need to implement new input daily_upload_hour as well
        # this will currently update hourly. will monitor
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=num_days)  # type: ignore
        bucket_filter_template = "year={yy}/month={mm}/day={dd}/"
        # self.remote_input_location.bucket = 'mbta-ctd-dataplatform-staging-springboard'
        s3_uris = file_list_from_s3_date_range(
            bucket_name=self.remote_input_location.bucket,
            file_prefix=self.remote_input_location.prefix,
            path_template=bucket_filter_template,
            end_date=end_date,
            start_date=start_date,
        )

        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )
        process_logger = ProcessLogger("filtered_hyper_create", num_days=num_days)
        process_logger.log_start()
        if len(ds_paths) == 0:
            process_logger.add_metadata(n_paths_zero=len(ds_paths))
            return False
        max_alloc = 0
        read_schema = list(set(ds.schema.names).intersection(self.output_processed_schema.names))

        dropped_columns = list(set(ds.schema.names).difference(self.output_processed_schema.names))
        added_columns = list(set(self.output_processed_schema.names).difference(ds.schema.names))

        with pq.ParquetWriter(self.local_parquet_path, schema=self.output_processed_schema) as writer:
            for batch in ds.to_batches(
                batch_size=500_000,
                columns=read_schema,
                filter=self.parquet_filter,
                batch_readahead=1,
                fragment_readahead=0,
            ):
                # don't write empty batch if no rows
                if batch.num_rows == 0:
                    continue

                # apply transformations if function passed in
                if self.dataframe_filter is not None:
                    polars_df = pl.from_arrow(batch)

                    if not isinstance(polars_df, pl.DataFrame):
                        raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

                    for col in added_columns:
                        polars_df = polars_df.with_columns(pl.lit(None).alias(col))

                    # filter, then reorder the columns to get them in pyarrow write order,
                    # otherwise the write_table call fails
                    polars_df = self.dataframe_filter(polars_df).select(writer.schema.names)

                    writer.write_table(polars_df.to_arrow())
                else:
                    # just write the batch out - filtered on columns of interest
                    writer.write_batch(batch)

                alloc = pyarrow.total_allocated_bytes()
                if alloc > max_alloc:
                    max_alloc = alloc
                    process_logger.add_metadata(alloc_bytes=max_alloc)

            process_logger.add_metadata(
                first_file=ds_paths[0],
                last_file=ds_paths[-1],
                tableau_writer_dropped_columns=dropped_columns,
                tableau_writer_added_columns=added_columns,
            )

        process_logger.log_complete()
        return True
