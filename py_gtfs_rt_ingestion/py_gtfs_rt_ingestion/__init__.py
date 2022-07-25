"""
This module is used by CTD's LAMP application to process rt gtfs json formatted
data and convert it into a parquet format for more efficient data analysis.
"""

from .batcher import batch_files, unpack_filenames
from .converter import ConfigType
from .converter_factory import get_converter
from .error import ArgumentException
from .lambda_types import LambdaContext, LambdaDict
from .s3_utils import file_list_from_s3, move_s3_objects

__version__ = "0.1.0"

DEFAULT_S3_PREFIX = "lamp"
