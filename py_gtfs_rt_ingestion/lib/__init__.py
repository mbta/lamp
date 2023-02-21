"""
This module is used by CTD's LAMP application to process rt gtfs json formatted
data and convert it into a parquet format for more efficient data analysis.
"""

from .converter import ConfigType
from .error import ArgumentException
from .ingest import ingest_files
from .logging_utils import ProcessLogger
from .postgres_utils import start_rds_writer_process
from .s3_utils import file_list_from_s3, move_s3_objects, write_parquet_file
from .utils import load_environment, group_sort_file_list, DEFAULT_S3_PREFIX
from .ecs import check_for_sigterm, handle_ecs_sigterm

__version__ = "0.1.0"
