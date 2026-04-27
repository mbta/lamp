from abc import ABC
from typing import List

import dataframely as dy
import msgspec
import polars as pl

from lamp_py.ingestion.gtfs_rt_structs import FeedMessage, GTFSRealtimeTable
from lamp_py.runtime_utils.remote_files import S3Location
from lamp_py.utils.typing import struct_to_schema


class GTFSRTDetail(ABC):
    """
    Abstract Base Class for all GTFSRTDetail implementations.

    GTFSRTDetail classes must implement all methods and properties that are
    defined.
    """

    record_schema: type[FeedMessage]
    table_schema: type[GTFSRealtimeTable]
    remote_location: S3Location

    def flatten_record(self, records: List[FeedMessage]) -> dy.LazyFrame[GTFSRealtimeTable]:
        """Flatten the GTFS Realtime message depending on its type."""
        jsons = msgspec.json.encode(records)
        lf = (
            pl.read_json(jsons, schema=struct_to_schema(self.record_schema).to_polars_schema())
            .lazy()
            .select("entity", pl.col("header").struct.field("timestamp").alias("feed_timestamp"))
            .explode("entity")
            .unnest("entity")
            .unnest(separator=".")
            .unnest(separator=".")
            .explode("trip_update.stop_time_update")
            .unnest("trip_update.stop_time_update", separator=".")
            .unnest(separator=".")
        )
        valid = self.table_schema.validate(lf, eager=False, cast=True)

        return valid
