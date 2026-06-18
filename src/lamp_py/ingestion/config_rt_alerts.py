from typing import List, Tuple

import dataframely as dy
import pyarrow

from lamp_py.utils.dataframely import with_alias, with_nullable

from .gtfs_rt_detail import FeedMessage, FeedEntityTable, GTFSRTDetail
from .gtfs_rt_structs import (
    translated_string,
    trip_descriptor,
)


class AlertsRecord(FeedMessage):
    """Each Alert message from GTFS-RT."""

    entity = dy.List(
        inner=dy.Struct(
            inner={
                "id": dy.String(min_length=1),
                "alert": dy.Struct(
                    inner={
                        "active_period": dy.List(
                            dy.Struct(
                                inner={
                                    "start": dy.UInt64(nullable=True),
                                    "end": dy.UInt64(nullable=True),
                                }
                            ),
                            nullable=True,
                        ),
                        "informed_entity": dy.List(
                            dy.Struct(
                                inner={
                                    "agency_id": dy.String(nullable=True),
                                    "route_id": dy.String(nullable=True),
                                    "route_type": dy.Int32(nullable=True),
                                    "direction_id": dy.UInt8(nullable=True),
                                    "trip": with_nullable(trip_descriptor, nullable=True),
                                    "stop_id": dy.String(nullable=True),
                                    "facility_id": dy.String(nullable=True),
                                    "activities": dy.List(dy.String(), nullable=True),  # MBTA Enhanced field
                                }
                            ),
                            nullable=True,
                        ),
                        "cause": dy.String(nullable=False),
                        "cause_detail": dy.String(nullable=True),  # type does not match spec type of <TranslatedString>
                        "effect": dy.String(nullable=True),
                        "effect_detail": dy.String(
                            nullable=True
                        ),  # type does not match spec type of <TranslatedString>
                        "url": translated_string,
                        "header_text": translated_string,
                        "description_text": translated_string,
                        "severity_level": dy.String(nullable=True),
                        "severity": dy.UInt16(nullable=True),  # MBTA Enhanced field
                        "created_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "last_modified_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "last_push_notification_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "closed_timestamp": dy.Int64(nullable=True),  # MBTA Enhanced field
                        "alert_lifecycle": dy.String(nullable=True),  # MBTA Enhanced field
                        "duration_certainty": dy.String(nullable=True),  # MBTA Enhanced field
                        "reminder_times": dy.List(dy.UInt64(), nullable=True),  # MBTA Enhanced field
                        "short_header_text": translated_string,  # not in message Alert struct spec
                        "service_effect_text": translated_string,  # MBTA Enhanced field
                        "timeframe_text": translated_string,  # MBTA Enhanced field
                        "recurrence_text": translated_string,  # MBTA Enhanced field
                    },
                    nullable=False,
                ),
            }
        ),
        min_length=1,
    )


class ProposedAlertsRecord(FeedMessage):
    """Each Alert message from GTFS-RT."""

    entity = dy.List(
        inner=dy.Struct(
            inner={
                "id": dy.String(min_length=1),
                "alert": dy.Struct(
                    inner={
                        "active_period": dy.List(
                            dy.Struct(
                                inner={
                                    "start": dy.UInt64(nullable=True),
                                    "end": dy.UInt64(nullable=True),
                                }
                            ),
                            nullable=True,
                        ),
                        "informed_entity": dy.List(
                            dy.Struct(
                                inner={
                                    "agency_id": dy.String(nullable=True),
                                    "route_id": dy.String(nullable=True),
                                    "route_type": dy.Int32(nullable=True),
                                    "direction_id": dy.UInt8(nullable=True),
                                    "trip": with_nullable(trip_descriptor, nullable=True),
                                    "stop_id": dy.String(nullable=True),
                                    "facility_id": dy.String(nullable=True),
                                    "activities": dy.List(dy.String(), nullable=True),  # MBTA Enhanced field
                                }
                            ),
                            nullable=True,
                        ),
                        "cause": dy.String(nullable=False),
                        "cause_detail": translated_string,
                        "effect": dy.String(nullable=True),
                        "effect_detail": translated_string,
                        "url": translated_string,
                        "header_text": translated_string,
                        "description_text": translated_string,
                        "severity_level": dy.String(nullable=True),
                        "severity": dy.UInt16(nullable=True),  # MBTA Enhanced field
                        "created_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "last_modified_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "last_push_notification_timestamp": dy.UInt64(nullable=True),  # MBTA Enhanced field
                        "closed_timestamp": dy.Int64(nullable=True),  # MBTA Enhanced field
                        "alert_lifecycle": dy.String(nullable=True),  # MBTA Enhanced field
                        "duration_certainty": dy.String(nullable=True),  # MBTA Enhanced field
                        "reminder_times": dy.List(dy.UInt64(), nullable=True),  # MBTA Enhanced field
                        "short_header_text": translated_string,  # not in message Alert struct spec
                        "service_effect_text": translated_string,  # MBTA Enhanced field
                        "timeframe_text": translated_string,  # MBTA Enhanced field
                        "recurrence_text": translated_string,  # MBTA Enhanced field
                    },
                    nullable=False,
                ),
            }
        ),
        min_length=1,
    )


class AlertsTable(FeedEntityTable):
    """Flattened Alerts data."""

    alert_cause = dy.String(alias="alert.cause")
    alert_cause_detail = dy.String(nullable=True, alias="alert.cause_detail")
    alert_cause_detail_translation = with_alias(
        translated_string.inner["translation"], "alert.cause_detail.translation"
    )
    alert_effect = dy.String(nullable=True, alias="alert.effect")
    alert_effect_detail = dy.String(nullable=True, alias="alert.effect_detail")
    alert_effect_detail_translation = with_alias(
        translated_string.inner["translation"], "alert.effect_detail.translation"
    )
    alert_severity_level = dy.String(nullable=True, alias="alert.severity_level")
    alert_severity = dy.UInt16(nullable=True, alias="alert.severity")
    alert_created_timestamp = dy.UInt64(nullable=True, alias="alert.created_timestamp")
    alert_last_modified_timestamp = dy.UInt64(nullable=True, alias="alert.last_modified_timestamp")
    alert_last_push_notification_timestamp = dy.UInt64(nullable=True, alias="alert.last_push_notification_timestamp")
    alert_closed_timestamp = dy.Int64(nullable=True, alias="alert.closed_timestamp")
    alert_alert_lifecycle = dy.String(nullable=True, alias="alert.alert_lifecycle")
    alert_duration_certainty = dy.String(nullable=True, alias="alert.duration_certainty")
    alert_active_period = dy.List(
        dy.Struct(
            inner={
                "start": dy.UInt64(nullable=True),
                "end": dy.UInt64(nullable=True),
            }
        ),
        nullable=True,
        alias="alert.active_period",
    )
    alert_informed_entity = dy.List(
        dy.Struct(
            inner={
                "agency_id": dy.String(nullable=True),
                "route_id": dy.String(nullable=True),
                "route_type": dy.Int32(nullable=True),
                "direction_id": dy.UInt8(nullable=True),
                "trip": with_nullable(trip_descriptor, True),
                "stop_id": dy.String(nullable=True),
                "facility_id": dy.String(nullable=True),
                "activities": dy.List(dy.String(), nullable=True),
            }
        ),
        nullable=True,
        alias="alert.informed_entity",
    )
    alert_url = with_alias(translated_string.inner["translation"], "alert.url.translation")
    alert_header_text = with_alias(translated_string.inner["translation"], "alert.header_text.translation")
    alert_description_text = with_alias(translated_string.inner["translation"], "alert.description_text.translation")
    alert_reminder_times = dy.List(dy.UInt64(), nullable=True, alias="alert.reminder_times")
    alert_short_header_text = with_alias(translated_string.inner["translation"], "alert.short_header_text.translation")
    alert_service_effect_text = with_alias(
        translated_string.inner["translation"], "alert.service_effect_text.translation"
    )
    alert_timeframe_text = with_alias(translated_string.inner["translation"], "alert.timeframe_text.translation")
    alert_recurrence_text = with_alias(translated_string.inner["translation"], "alert.recurrence_text.translation")


class RtAlertsDetail(GTFSRTDetail[AlertsTable, AlertsRecord]):
    """
    Detail for how to convert RT GTFS Alerts from json entries into parquet
    tables.
    """

    @property
    def table_schema(self) -> type[AlertsTable]:
        return AlertsTable

    @property
    def record_schema(self) -> type[AlertsRecord]:
        return AlertsRecord

    @property
    def partition_column(self) -> str:
        return "alert.cause"

    @property
    def import_schema(self) -> pyarrow.schema:
        return pyarrow.schema([v.pyarrow_field(k) for k, v in self.record_schema.entity.inner.inner.items()])  # type: ignore[attr-defined]

    @property
    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("alert.severity", "ascending"),
            ("alert.effect", "ascending"),
            ("feed_timestamp", "ascending"),
        ]

    def transform_for_write(self, table: pyarrow.Table) -> pyarrow.Table:
        """Flatten table, then add columns defined in table_schema if they aren't already in the flattened table."""
        table = super().transform_for_write(table)
        expected_table = AlertsTable.create_empty().to_arrow()
        unioned_table = pyarrow.concat_tables([table, expected_table], promote_options="permissive")
        return unioned_table
