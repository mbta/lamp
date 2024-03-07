from typing import List, Tuple
import pyarrow

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import (
    trip_descriptor,
    translated_string,
)


class RtAlertsDetail(GTFSRTDetail):
    """
    Detail for how to convert RT GTFS Alerts from json entries into parquet
    tables.
    """

    @property
    def import_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("id", pyarrow.string()),
                (
                    "alert",
                    pyarrow.struct(
                        [
                            (
                                "active_period",
                                pyarrow.list_(
                                    pyarrow.struct(
                                        [
                                            ("start", pyarrow.uint64()),
                                            ("end", pyarrow.uint64()),
                                        ]
                                    )
                                ),
                            ),
                            (
                                "informed_entity",
                                pyarrow.list_(
                                    pyarrow.struct(
                                        [
                                            ("agency_id", pyarrow.string()),
                                            ("route_id", pyarrow.string()),
                                            ("route_type", pyarrow.int32()),
                                            ("direction_id", pyarrow.uint8()),
                                            ("trip", trip_descriptor),
                                            ("stop_id", pyarrow.string()),
                                            ("facility_id", pyarrow.string()),
                                            (
                                                "activities",
                                                pyarrow.list_(pyarrow.string()),
                                            ),  # MBTA Enhanced field
                                        ]
                                    )
                                ),
                            ),
                            ("cause", pyarrow.string()),
                            (
                                "cause_detail",
                                pyarrow.string(),
                            ),  # type does not match spec type of <TranslatedString>
                            ("effect", pyarrow.string()),
                            (
                                "effect_detail",
                                pyarrow.string(),
                            ),  # type does not match spec type of <TranslatedString>
                            ("url", translated_string),
                            ("header_text", translated_string),
                            ("description_text", translated_string),
                            ("severity_level", pyarrow.string()),
                            (
                                "severity",
                                pyarrow.uint16(),
                            ),  # not in message Alert struct spec
                            (
                                "created_timestamp",
                                pyarrow.uint64(),
                            ),  # not in message Alert struct spec
                            (
                                "last_modified_timestamp",
                                pyarrow.uint64(),
                            ),  # not in message Alert struct spec
                            (
                                "last_push_notification_timestamp",
                                pyarrow.uint64(),
                            ),  # not in message Alert struct spec
                            (
                                "closed_timestamp",
                                pyarrow.int64(),
                            ),  # not in message Alert struct spec
                            (
                                "alert_lifecycle",
                                pyarrow.string(),
                            ),  # not in message Alert struct spec
                            (
                                "duration_certainty",
                                pyarrow.string(),
                            ),  # not in message Alert struct spec
                            (
                                "reminder_times",
                                pyarrow.list_(pyarrow.uint64()),
                            ),  # not in message Alert struct spec
                            (
                                "short_header_text",
                                translated_string,
                            ),  # not in message Alert struct spec
                            (
                                "service_effect_text",
                                translated_string,
                            ),  # not in message Alert struct spec
                            (
                                "timeframe_text",
                                translated_string,
                            ),  # not in message Alert struct spec
                            (
                                "recurrence_text",
                                translated_string,
                            ),  # not in message Alert struct spec
                        ]
                    ),
                ),
            ]
        )

    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("alert.severity", "ascending"),
            ("alert.effect", "ascending"),
            ("alert.cause", "ascending"),
            ("feed_timestamp", "ascending"),
        ]
