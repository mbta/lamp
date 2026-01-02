import polars as pl
import os
from datetime import date
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_ARCHIVE,
    S3Location,
    bus_events,
)
from lamp_py.tableau.jobs.bus_performance import bus_schema
from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions

GTFS_RT_TABLEAU_PROJECT = "GTFS-RT"


tableau_bus_fall_rating = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "bus_rating_datasets", "year=2025", "Fall.parquet"), version="1.0"
)

HyperBus = FilteredHyperJob(
    remote_input_location=bus_events,
    remote_output_location=tableau_bus_fall_rating,
    start_date=date(2025, 8, 24),
    # start_date=date(2025, 12,12),
    end_date=date(2025, 12, 13),
    processed_schema=bus_schema,
    dataframe_filter=apply_bus_analysis_conversions,
    parquet_filter=None,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    partition_template="{yy}{mm:02d}{dd:02d}.parquet",
)

HyperBus.run_parquet(None)
