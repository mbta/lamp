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
                            ("current_status", pyarrow.string()),
                            ("current_stop_sequence", pyarrow.uint32()),
                            ("occupancy_percentage", pyarrow.uint32()),
                            ("occupancy_status", pyarrow.string()),
                            ("stop_id", pyarrow.string()),
                            ("timestamp", pyarrow.uint64()),
                            ("position", position),
                            ("trip", trip_descriptor),
                            ("vehicle", vehicle_descriptor),
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
