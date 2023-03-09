import pyarrow

from .gtfs_rt_detail import GTFSRTDetail


class RtTripDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Trip Updates from json entries into
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
                # entity -> trip_update
                ("timestamp", pyarrow.int64()),
                (
                    "stop_time_update",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field(
                                    "departure",
                                    pyarrow.struct(
                                        [
                                            pyarrow.field(
                                                "time", pyarrow.int64()
                                            ),
                                            pyarrow.field(
                                                "uncertainty", pyarrow.int64()
                                            ),
                                        ]
                                    ),
                                ),
                                pyarrow.field("stop_id", pyarrow.string()),
                                pyarrow.field("stop_sequence", pyarrow.int64()),
                                pyarrow.field(
                                    "arrival",
                                    pyarrow.struct(
                                        [
                                            pyarrow.field(
                                                "time", pyarrow.int64()
                                            ),
                                            pyarrow.field(
                                                "uncertainty", pyarrow.int64()
                                            ),
                                        ]
                                    ),
                                ),
                                pyarrow.field(
                                    "schedule_relationship", pyarrow.string()
                                ),
                                pyarrow.field(
                                    "boarding_status", pyarrow.string()
                                ),
                            ]
                        )
                    ),
                ),
                # entity -> trip_update -> trip
                ("direction_id", pyarrow.int64()),
                ("route_id", pyarrow.string()),
                ("start_date", pyarrow.string()),
                ("start_time", pyarrow.string()),
                ("trip_id", pyarrow.string()),
                ("route_pattern_id", pyarrow.string()),
                ("schedule_relationship", pyarrow.string()),
                # entity -> trip_update -> vehicle
                ("vehicle_id", pyarrow.string()),  # actual label: id
                ("vehicle_label", pyarrow.string()),  # actual label: label
            ]
        )

    @property
    def transformation_schema(self) -> dict:
        return {
            "entity": (("id", "entity_id"),),
            "entity,trip_update": (
                ("timestamp",),
                ("stop_time_update",),
            ),
            "entity,trip_update,trip": (
                ("direction_id",),
                ("route_id",),
                ("start_date",),
                ("start_time",),
                ("trip_id",),
                ("route_pattern_id",),
                ("schedule_relationship",),
            ),
            "entity,trip_update,vehicle": (
                (
                    "id",
                    "vehicle_id",
                ),
                (
                    "label",
                    "vehicle_label",
                ),
            ),
        }
