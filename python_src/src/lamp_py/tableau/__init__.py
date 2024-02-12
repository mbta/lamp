"""Utilities for Interacting with Tableau and Hyper files"""

import logging
from types import ModuleType
from typing import Optional

from lamp_py.postgres.postgres_utils import DatabaseManager

# pylint: disable=C0103 (invalid-name)
# pylint wants pipeline to conform to an UPPER_CASE constant naming style. its
# a module though, so disabling to allow it to use normal import rules.
pipeline: Optional[ModuleType]

try:
    from . import pipeline
except ModuleNotFoundError:
    pipeline = None

# pylint: enable=C0103 (invalid-name)


def start_parquet_updates(db_manager: DatabaseManager) -> None:
    """
    wrapper around pipeline.start_parquet_updates function. if a module not
    found error occurs (which happens when using osx arm64 dependencies), log
    an error and do nothing. else, run the function.
    """
    if pipeline is None:
        logging.error(
            "Unable to run parquet files on this machine due to Module Not Found error"
        )
    else:
        pipeline.start_parquet_updates(db_manager=db_manager)
