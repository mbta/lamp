from abc import ABC

import dataframely as dy
import polars as pl

from lamp_py.ingestion.gtfs_rt_structs import FeedMessage, GTFSRealtimeTable
from lamp_py.runtime_utils.remote_files import S3Location


class GTFSRTDetail(ABC):
    """
    Abstract Base Class for all GTFSRTDetail implementations.

    GTFSRTDetail classes must implement all methods and properties that are
    defined.
    """

    record_schema: type[FeedMessage]
    table_schema: type[GTFSRealtimeTable]
    remote_location: S3Location

    def flatten_record(self, lf: pl.LazyFrame) -> dy.LazyFrame[GTFSRealtimeTable]:
        """Flatten the GTFS Realtime message depending on its type."""
        valid = self.table_schema.validate(lf, eager=False, cast=True)

        return valid
