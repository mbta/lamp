from typing import List, Tuple
import pyarrow

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import (
    trip_descriptor,
    vehicle_descriptor,
    stop_time_event,
)


class RtTripDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Trip Updates from json entries into
    parquet tables.
    """

    def transform_for_write(self, table: pyarrow.table) -> pyarrow.table:
        """modify table schema before write to parquet"""
        return self.flatten_schema(
            self.explode_table_column(
                self.flatten_schema(table), "trip_update.stop_time_update"
            )
        )

    @property
    def import_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("id", pyarrow.string()),
                (
                    "trip_update",
                    pyarrow.struct(
                        [
                            ("timestamp", pyarrow.uint64()),
                            ("delay", pyarrow.int32()),
                            ("trip", trip_descriptor),
                            ("vehicle", vehicle_descriptor),
                            (
                                "stop_time_update",
                                pyarrow.list_(
                                    pyarrow.struct(
                                        [
                                            ("stop_sequence", pyarrow.uint32()),
                                            ("stop_id", pyarrow.string()),
                                            ("arrival", stop_time_event),
                                            ("departure", stop_time_event),
                                            (
                                                "schedule_relationship",
                                                pyarrow.string(),
                                            ),
                                            (
                                                "boarding_status",
                                                pyarrow.string(),
                                            ),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )

    # pylint: disable=R0801
    # Similar lines in 2 files
    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("trip_update.trip.start_date", "ascending"),
            ("trip_update.trip.route_pattern_id", "ascending"),
            ("trip_update.trip.route_id", "ascending"),
            ("trip_update.trip.direction_id", "ascending"),
            ("trip_update.vehicle.id", "ascending"),
            ("feed_timestamp", "ascending"),
        ]

    # pylint: enable=R0801
