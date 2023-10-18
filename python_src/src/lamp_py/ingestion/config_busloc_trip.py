from typing import List, Tuple
import pyarrow

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import (
    trip_descriptor,
    vehicle_descriptor,
    stop_time_event,
)


class RtBusTripDetail(GTFSRTDetail):
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
                            (
                                "timestamp",
                                pyarrow.uint64(),
                            ),  # Not currently provided by Busloc
                            (
                                "delay",
                                pyarrow.int32(),
                            ),  # Not currently provided by Busloc
                            (
                                "trip",
                                trip_descriptor,
                            ),  # Busloc currently only provides trip_id, route_id and schedule_relationship
                            (
                                "vehicle",
                                vehicle_descriptor,
                            ),  # Busloc currently only provides id and label
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
                                            ("cause_id", pyarrow.uint16()),
                                            (
                                                "cause_description",
                                                pyarrow.string(),
                                            ),
                                            ("remark", pyarrow.string()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    ),
                ),
            ]
        )

    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("trip_update.trip.direction_id", "ascending"),
            ("trip_update.vehicle.id", "ascending"),
            ("feed_timestamp", "ascending"),
        ]
