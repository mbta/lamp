"""
This module is used by CTD's LAMP application to process gtfs parquet files to
analize the performance of the MBTA system.
"""

from .l0_gtfs_rt_events import process_gtfs_rt_files
from .l0_gtfs_static_table import process_static_tables
from .l1_rt_metrics import process_trips_and_metrics

__version__ = "0.1.0"
