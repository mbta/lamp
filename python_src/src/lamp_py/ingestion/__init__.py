"""
This module is used by CTD's LAMP application to process rt gtfs json formatted
data and convert it into a parquet format for more efficient data analysis.
"""

from .converter import ConfigType
from .error import ArgumentException
from .ingest import ingest_files
from .utils import group_sort_file_list, DEFAULT_S3_PREFIX

__version__ = "0.1.0"
