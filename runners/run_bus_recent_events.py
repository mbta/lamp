#!/usr/bin/env python
from datetime import datetime, timedelta
from typing import List
from lamp_py.bus_performance_manager.event_files import event_files_to_load
from lamp_py.bus_performance_manager.events_metrics import bus_performance_metrics
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
from lamp_py.tableau.jobs.bus_performance import bus_schema_recent

# stage 1 - regenerate range
##################
# this is a good runner - 6/17/25
end_date = datetime(year=2025, month=6, day=17)
start_date = end_date - timedelta(days=7)
# write_bus_metrics(start_date, end_date, write_local_only=True)
##################
ss = pl.date_range(start_date, end_date, "1d", eager=True)

# stage 2 - concatenate and apply transforms
# def create_bus_parquet(job: HyperJob, num_files: Optional[int]) -> None:
"""
Join bus_events files into single parquet file for upload to Tableau
"""
ds_paths = [f"/tmp/{service_date.strftime('%Y%m%d')}.parquet" for service_date in ss]

ds = pd.dataset(
    ds_paths,
    format="parquet",
    filesystem=None,
)

with pq.ParquetWriter("/tmp/bus_recent.parquet", schema=bus_schema_recent) as writer:
    for batch in ds.to_batches(batch_size=500_000, batch_readahead=1, fragment_readahead=0):
        # this select() is here to make sure the order of the polars_df
        # schema is the same as the bus_schema above.
        # order of schema matters to the ParquetWriter

        # if the bus_schema above is in the same order as the batch
        # schema, then the select will do nothing - as expected

        polars_df = pl.from_arrow(batch).select(bus_schema_recent.names)  # type: ignore[union-attr]

        if not isinstance(polars_df, pl.DataFrame):
            raise TypeError(f"Expected a Polars DataFrame or Series, but got {type(polars_df)}")

        writer.write_table(apply_bus_analysis_conversions(polars_df))
