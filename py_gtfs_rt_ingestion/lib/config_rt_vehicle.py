import pyarrow

from .gtfs_rt_detail import GTFSRTDetail


class RtVehicleDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Vehicle Positions from json entries into
    parquet tables.
    """

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                # header -> timestamp
                ("year", pyarrow.int16()),
                ("month", pyarrow.int8()),
                ("day", pyarrow.int8()),
                ("hour", pyarrow.int8()),
                ("feed_timestamp", pyarrow.int64()),
                # entity
                ("entity_id", pyarrow.string()),  # actual label: id
                # entity -> vehicle
                ("current_status", pyarrow.string()),
                ("current_stop_sequence", pyarrow.int64()),
                ("occupancy_percentage", pyarrow.int64()),
                ("occupancy_status", pyarrow.string()),
                ("stop_id", pyarrow.string()),
                (
                    "vehicle_timestamp",
                    pyarrow.int64(),
                ),  # actual label: timestamp
                # entity -> vehicle -> position
                ("bearing", pyarrow.int64()),
                ("latitude", pyarrow.float64()),
                ("longitude", pyarrow.float64()),
                ("speed", pyarrow.float64()),
                # entity -> vehicle -> trip
                ("direction_id", pyarrow.int64()),
                ("route_id", pyarrow.string()),
                ("schedule_relationship", pyarrow.string()),
                ("start_date", pyarrow.string()),
                ("start_time", pyarrow.string()),
                ("trip_id", pyarrow.string()),  # actual label: id
                # entity -> vehicle -> vehicle
                ("vehicle_id", pyarrow.string()),  # actual label: id
                ("vehicle_label", pyarrow.string()),  # actual label: label
                (
                    "vehicle_consist",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("label", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: consist
            ]
        )

    @property
    def transformation_schema(self) -> dict:
        return {
            "entity": (("id", "entity_id"),),
            "entity,vehicle": (
                ("current_status",),
                ("current_stop_sequence",),
                ("occupancy_percentage",),
                ("occupancy_status",),
                ("stop_id",),
                ("timestamp", "vehicle_timestamp"),
            ),
            "entity,vehicle,position": (
                ("bearing",),
                ("latitude",),
                ("longitude",),
                ("speed",),
            ),
            "entity,vehicle,trip": (
                ("direction_id",),
                ("route_id",),
                ("schedule_relationship",),
                ("start_date",),
                ("start_time",),
                ("id", "trip_id"),
            ),
            "entity,vehicle,vehicle": (
                (
                    "id",
                    "vehicle_id",
                ),
                (
                    "label",
                    "vehicle_label",
                ),
                (
                    "consist",
                    "vehicle_consist",
                ),
            ),
        }
