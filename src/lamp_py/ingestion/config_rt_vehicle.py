from typing import List, Tuple
import pyarrow

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import position, trip_descriptor, vehicle_descriptor


class RtVehicleDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Vehicle Positions from json entries into
    parquet tables.
    """

    @property
    def import_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("id", pyarrow.string()),
                (
                    "vehicle",
                    pyarrow.struct(
                        [
                            ("trip", trip_descriptor),
                            ("vehicle", vehicle_descriptor),
                            ("position", position),
                            ("current_stop_sequence", pyarrow.uint32()),
                            ("stop_id", pyarrow.string()),
                            ("current_status", pyarrow.string()),
                            ("timestamp", pyarrow.uint64()),
                            ("congestion_level", pyarrow.string()),
                            ("occupancy_status", pyarrow.string()),
                            ("occupancy_percentage", pyarrow.uint32()),
                            (
                                "multi_carriage_details",
                                pyarrow.list_(
                                    pyarrow.struct(
                                        [
                                            ("id", pyarrow.string()),
                                            ("label", pyarrow.string()),
                                            (
                                                "occupancy_status",
                                                pyarrow.string(),
                                            ),
                                            (
                                                "occupancy_percentage",
                                                pyarrow.int32(),
                                            ),
                                            (
                                                "carriage_sequence",
                                                pyarrow.uint32(),
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

    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("vehicle.trip.start_date", "ascending"),
            ("vehicle.trip.route_id", "ascending"),
            ("vehicle.vehicle.id", "ascending"),
            ("vehicle.trip.direction_id", "ascending"),
            ("feed_timestamp", "ascending"),
        ]
