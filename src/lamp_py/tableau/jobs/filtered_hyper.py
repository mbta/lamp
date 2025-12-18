from typing import Optional, Callable
from datetime import date, datetime, timedelta
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc
from pyarrow.fs import S3FileSystem

import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.remote_files import S3Location

from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_date_range


def days_ago(num_days: int) -> date:
    """helper function to get a date() object set to num_days ago"""
    return (datetime.now() - timedelta(days=num_days)).date()


# pylint: disable=R0917,R0902,R0913
# pylint too many local variables (more than 15)
class FilteredHyperJob(HyperJob):
    """HyperJob for Generic GTFS RT conversion - Converting from OOP to Composition"""

    def __init__(
        self,
        remote_input_location: S3Location,
        remote_output_location: S3Location,
        processed_schema: pyarrow.schema,
        tableau_project_name: str,
        start_date: date | None = days_ago(7),  # default this the past week of data
        end_date: date | None = datetime.now().date(),
        partition_template: str = "year={yy}/month={mm}/day={dd}/",
        parquet_preprocess: Callable[[pyarrow.Table], pyarrow.Table] | None = None,
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
        self.partition_template = partition_template

        if start_date is not None and end_date is not None:
            assert start_date <= end_date

        self.start_date = start_date
        self.end_date = end_date

        self.parquet_preprocess = parquet_preprocess  # level 1 | complex preprocess
        self.parquet_filter = parquet_filter  # level 2 | by column and simple filter
        self.dataframe_filter = dataframe_filter  # level 3 | complex filter

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return self.processed_schema

    def create_parquet(self, _: DatabaseManager | None) -> None:
        self.update_parquet(None)

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        return self.create_tableau_parquet(partition_template=self.partition_template)

    # pylint: disable=R0914, R0912
    # pylint too many local variables (more than 15)
    def create_tableau_parquet(self, partition_template: str) -> bool:
        """
        Join files into single parquet file for upload to Tableau. apply filter and conversions as necessary

        Parameters
        ----------
        num_days : Number of days to query and concatenate. If None, processes all days available

        Returns
        -------
        True if parquet created, False otherwise
        """
        if self.start_date is not None and self.end_date is not None:
            # limitation of filtered hyper only does whole days.
            # update to allow start/end set by hour, to get the entire
            # previous days uploads working
            # need to implement new input daily_upload_hour as well
            # this will currently update hourly. will monitor
            s3_uris = file_list_from_s3_date_range(
                bucket_name=self.remote_input_location.bucket,
                file_prefix=self.remote_input_location.prefix,
                path_template=partition_template,
                end_date=self.end_date,
                start_date=self.start_date,
            )
        else:
            s3_uris = file_list_from_s3(
                bucket_name=self.remote_input_location.bucket,
                file_prefix=self.remote_input_location.prefix,
            )
        ds_paths = [s.replace("s3://", "") for s in s3_uris]

        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=S3FileSystem(),
        )
        process_logger = ProcessLogger(
            "filtered_hyper_create_parquet", start_date=self.start_date, end_date=self.end_date
        )
        process_logger.log_start()
        if len(ds_paths) == 0:
            process_logger.add_metadata(n_paths_zero=len(ds_paths))
            return False
        max_alloc = 0

        dropped_columns = list(set(ds.schema.names).difference(self.output_processed_schema.names))
        added_columns = list(set(self.output_processed_schema.names).difference(ds.schema.names))

        with pq.ParquetWriter(self.local_parquet_path, schema=self.output_processed_schema) as writer:
            for batch in ds.to_batches(
                batch_size=500_000,
                columns=ds.schema.names,
                filter=self.parquet_filter,
                batch_readahead=1,
                fragment_readahead=0,
            ):
                # don't check empty batch if no rows
                if batch.num_rows == 0:
                    continue

                # apply transformations if function passed in
                if self.parquet_preprocess is not None:
                    table = pyarrow.Table.from_batches([batch])
                    batch = self.parquet_preprocess(table)

                # apply transformations if function passed in
                if self.dataframe_filter is not None:
                    polars_df = pl.from_arrow(batch)

                    if not isinstance(polars_df, pl.DataFrame):
                        raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

                    # filter, then reorder the columns to get them in pyarrow write order,
                    # otherwise the write_table call fails
                    try:
                        # for methods that populate/explode dataframe...creating its own "added" cols.
                        polars_df = self.dataframe_filter(polars_df).select(writer.schema.names)
                    except Exception as _:
                        # # revisit...do i want this here?
                        # for methods that expect all the columns to be there already
                        for col in added_columns:
                            polars_df = polars_df.with_columns(pl.lit(None).alias(col))
                        polars_df = self.dataframe_filter(polars_df).select(writer.schema.names)

                    # don't write empty batch if no rows
                    if polars_df.height > 0:
                        writer.write_table(polars_df.to_arrow())
                else:
                    # filtered on self.parquet_filter and self.parquet_preprocess
                    if isinstance(batch, pyarrow.RecordBatch):
                        writer.write_batch(batch)
                    elif isinstance(batch, pyarrow.Table):
                        writer.write_table(batch)

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
