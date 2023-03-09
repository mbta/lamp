import pyarrow

from .gtfs_rt_detail import GTFSRTDetail


class RtAlertsDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Alerts from json entries into parquet
    tables.
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
                # entity -> alert
                ("effect", pyarrow.string()),
                ("effect_detail", pyarrow.string()),
                ("cause", pyarrow.string()),
                ("cause_detail", pyarrow.string()),
                ("severity", pyarrow.int64()),
                ("severity_level", pyarrow.string()),
                ("created_timestamp", pyarrow.int64()),
                ("last_modified_timestamp", pyarrow.int64()),
                ("alert_lifecycle", pyarrow.string()),
                ("duration_certainty", pyarrow.string()),
                ("last_push_notification", pyarrow.int64()),
                (
                    "active_period",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("start", pyarrow.int64()),
                                pyarrow.field("end", pyarrow.int64()),
                            ]
                        )
                    ),
                ),
                ("reminder_times", pyarrow.list_(pyarrow.int64())),
                ("closed_timestamp", pyarrow.int64()),
                # entity -> alert -> short_header_text
                (
                    "short_header_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> header_text
                (
                    "header_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> description_text
                (
                    "description_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> service_effect_text
                (
                    "service_effect_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> timeframe_text
                (
                    "timeframe_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> url
                (
                    "url_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                # entity -> alert -> recurrence_text
                (
                    "recurrence_text_translation",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("text", pyarrow.string()),
                                pyarrow.field("language", pyarrow.string()),
                            ]
                        )
                    ),
                ),  # actual label: translation
                (
                    "informed_entity",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("stop_id", pyarrow.string()),
                                pyarrow.field("facility_id", pyarrow.string()),
                                pyarrow.field(
                                    "activities",
                                    pyarrow.list_(pyarrow.string()),
                                ),
                                pyarrow.field("agency_id", pyarrow.string()),
                                pyarrow.field("route_type", pyarrow.int64()),
                                pyarrow.field("route_id", pyarrow.string()),
                                pyarrow.field(
                                    "trip",
                                    pyarrow.struct(
                                        [
                                            pyarrow.field(
                                                "route_id", pyarrow.string()
                                            ),
                                            pyarrow.field(
                                                "trip_id", pyarrow.string()
                                            ),
                                            pyarrow.field(
                                                "direcction_id", pyarrow.int64()
                                            ),
                                        ]
                                    ),
                                ),
                                pyarrow.field("direction_id", pyarrow.int64()),
                            ]
                        )
                    ),
                ),
            ]
        )

    @property
    def transformation_schema(self) -> dict:
        return {
            "entity": (("id", "entity_id"),),
            "entity,alert": (
                ("effect",),
                ("effect_detail",),
                ("cause",),
                ("cause_detail",),
                ("severity",),
                ("severity_level",),
                ("created_timestamp",),
                ("last_modified_timestamp",),
                ("alert_lifecycle",),
                ("duration_certainty",),
                ("last_push_notification",),
                ("active_period",),
                ("reminder_times",),
                ("closed_timestamp",),
                ("informed_entity",),
            ),
            "entity,alert,short_header_text": (
                ("translation", "short_header_text_translation"),
            ),
            "entity,alert,header_text": (
                ("translation", "header_text_translation"),
            ),
            "entity,alert,description_text": (
                ("translation", "description_text_translation"),
            ),
            "entity,alert,service_effect_text": (
                ("translation", "service_effect_text_translation"),
            ),
            "entity,alert,timeframe_text": (
                ("translation", "timeframe_text_translation"),
            ),
            "entity,alert,url": (("translation", "url_translation"),),
            "entity,alert,recurrence_text": (
                ("translation", "recurrence_text_translation"),
            ),
        }
