import pyarrow

from .gtfs_rt_detail import GTFSRTDetail


class RtBusVehicleDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Bus Vehicle Positions from json
    entries into parquet tables.
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
                (
                    "entity_is_deleted",
                    pyarrow.bool_(),
                ),  # actual label: is_deleted
                # entity -> vehicle
                ("block_id", pyarrow.string()),
                ("capacity", pyarrow.int64()),
                ("current_stop_sequence", pyarrow.int64()),
                ("load", pyarrow.int64()),
                ("location_source", pyarrow.string()),
                ("occupancy_percentage", pyarrow.int64()),
                ("occupancy_status", pyarrow.string()),
                ("revenue", pyarrow.bool_()),
                ("run_id", pyarrow.string()),
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
                ("overload_id", pyarrow.int64()),
                ("overload_offset", pyarrow.int64()),
                ("route_id", pyarrow.string()),
                ("schedule_relationship", pyarrow.string()),
                ("start_date", pyarrow.string()),
                ("trip_id", pyarrow.string()),
                # entity -> vehicle -> vehicle
                ("vehicle_id", pyarrow.string()),  # actual label: id
                ("vehicle_label", pyarrow.string()),  # actual label: label
                ("assignment_status", pyarrow.string()),
                # entity -> vehicle -> operator
                ("operator_id", pyarrow.string()),  # actual label: id
                ("logon_time", pyarrow.int64()),
                ("name", pyarrow.string()),
                ("first_name", pyarrow.string()),
                ("last_name", pyarrow.string()),
            ]
        )

    @property
    def transformation_schema(self) -> dict:
        return {
            "entity": (
                ("id", "entity_id"),
                ("is_deleted", "entity_is_deleted"),
            ),
            "entity,vehicle": (
                ("block_id",),
                ("capacity",),
                ("current_stop_sequence",),
                ("load",),
                ("location_source",),
                ("occupancy_percentage",),
                ("occupancy_status",),
                ("revenue",),
                ("run_id",),
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
                ("overload_id",),
                ("overload_offset",),
                ("route_id",),
                ("schedule_relationship",),
                ("start_date",),
                ("trip_id",),
            ),
            "entity,vehicle,vehicle": (
                ("id", "vehicle_id"),
                ("label", "vehicle_label"),
                ("assignment_status",),
            ),
            "entity,vehicle,operator": (
                ("id", "operator_id"),
                ("logon_time",),
                ("name",),
                ("first_name",),
                ("last_name",),
            ),
        }
