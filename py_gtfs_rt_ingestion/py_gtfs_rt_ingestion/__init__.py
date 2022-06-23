"""
This module is used by CTD's LAMP application to process rt gtfs json formatted
data and convert it into a parquet format for more efficient data analysis.
"""

from .batcher import batch_files
from .config_base import ConfigType
from .configuration import Configuration
from .convert_gtfs import zip_to_pyarrow
from .convert_gtfs_rt import gz_to_pyarrow
from .error import ArgumentException
from .error import ConfigTypeFromFilenameException
from .error import NoImplException
from .lambda_types import LambdaContext
from .lambda_types import LambdaDict
from .s3_utils import file_list_from_s3
from .s3_utils import move_s3_objects

__version__ = "0.1.0"

DEFAULT_S3_PREFIX = "lamp"
