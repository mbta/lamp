from typing import List, Tuple
import pyarrow

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import (
    position,
    vehicle_descriptor,
    trip_descriptor,
)


class RtBusVehicleDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Bus Vehicle Positions from json
    entries into parquet tables.
    """

    @property
    def partition_column(self) -> str:
        return "vehicle.trip.route_id"

    @property
    def import_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("id", pyarrow.string()),
                ("is_deleted", pyarrow.bool_()),
                (
                    "vehicle",
                    pyarrow.struct(
                        [
                            ("position", position),
                            ("location_source", pyarrow.string()),
                            ("timestamp", pyarrow.uint64()),
                            ("trip", trip_descriptor),
                            ("vehicle", vehicle_descriptor),
                            (
                                "operator",
                                pyarrow.struct(
                                    [
                                        ("id", pyarrow.string()),
                                        ("first_name", pyarrow.string()),
                                        ("last_name", pyarrow.string()),
                                        ("name", pyarrow.string()),
                                        ("logon_time", pyarrow.uint64()),
                                    ]
                                ),
                            ),
                            ("block_id", pyarrow.string()),
                            ("run_id", pyarrow.string()),
                            ("stop_id", pyarrow.string()),
                            ("current_stop_sequence", pyarrow.uint32()),
                            ("revenue", pyarrow.bool_()),
                            ("current_status", pyarrow.string()),
                            ("load", pyarrow.uint16()),
                            ("capacity", pyarrow.uint16()),
                            ("occupancy_percentage", pyarrow.uint16()),
                            ("occupancy_status", pyarrow.string()),
                        ]
                    ),
                ),
            ]
        )

    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("vehicle.block_id", "ascending"),
            ("vehicle.vehicle.id", "ascending"),
            ("feed_timestamp", "ascending"),
        ]
