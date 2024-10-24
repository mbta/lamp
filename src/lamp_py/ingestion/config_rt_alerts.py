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
    def partition_column(self) -> str:
        return "alert.cause"

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
                            ),  # MBTA Enhanced field
                            (
                                "created_timestamp",
                                pyarrow.uint64(),
                            ),  # MBTA Enhanced field
                            (
                                "last_modified_timestamp",
                                pyarrow.uint64(),
                            ),  # MBTA Enhanced field
                            (
                                "last_push_notification_timestamp",
                                pyarrow.uint64(),
                            ),  # MBTA Enhanced field
                            (
                                "closed_timestamp",
                                pyarrow.int64(),
                            ),  # MBTA Enhanced field
                            (
                                "alert_lifecycle",
                                pyarrow.string(),
                            ),  # MBTA Enhanced field
                            (
                                "duration_certainty",
                                pyarrow.string(),
                            ),  # MBTA Enhanced field
                            (
                                "reminder_times",
                                pyarrow.list_(pyarrow.uint64()),
                            ),  # MBTA Enhanced field
                            (
                                "short_header_text",
                                translated_string,
                            ),  # not in message Alert struct spec
                            (
                                "service_effect_text",
                                translated_string,
                            ),  # MBTA Enhanced field
                            (
                                "timeframe_text",
                                translated_string,
                            ),  # MBTA Enhanced field
                            (
                                "recurrence_text",
                                translated_string,
                            ),  # MBTA Enhanced field
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
            ("feed_timestamp", "ascending"),
        ]
