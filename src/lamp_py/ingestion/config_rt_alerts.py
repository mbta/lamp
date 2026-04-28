from typing import List

import dataframely as dy

from lamp_py.runtime_utils.remote_files import rt_alerts

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import Alert, FeedEntity, FeedMessage, GTFSRealtimeTable


class RtAlertEntity(FeedEntity, kw_only=True):
    """Each entity in the RT Alerts feed."""

    alert: Alert


class RtAlertMessage(FeedMessage):
    """A snapshot of the RT Alerts feed."""

    entity: List[RtAlertEntity]


class RtAlertTable(GTFSRealtimeTable):
    """Flattened RT Alerts data."""

    cause = dy.String(nullable=True, alias="alert.cause")
    cause_detail = dy.String(nullable=True, alias="alert.cause_detail")
    effect = dy.String(nullable=True, alias="alert.effect")
    effect_detail = dy.String(nullable=True, alias="alert.effect_detail")
    severity_level = dy.String(nullable=True, alias="alert.severity_level")
    severity = dy.UInt16(nullable=True, alias="alert.severity")
    created_timestamp = dy.UInt64(nullable=True, alias="alert.created_timestamp")
    last_modified_timestamp = dy.UInt64(nullable=True, alias="alert.last_modified_timestamp")
    last_push_notification_timestamp = dy.UInt64(nullable=True, alias="alert.last_push_notification_timestamp")
    closed_timestamp = dy.Int64(nullable=True, alias="alert.closed_timestamp")
    alert_lifecycle = dy.String(nullable=True, alias="alert.alert_lifecycle")
    duration_certainty = dy.String(nullable=True, alias="alert.duration_certainty")
    url = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.url.translation",
    )
    header_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.header_text.translation",
    )
    description_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.description_text.translation",
    )
    short_header_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.short_header_text.translation",
    )
    service_effect_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.service_effect_text.translation",
    )
    timeframe_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.timeframe_text.translation",
    )
    recurrence_text = dy.List(
        dy.Struct(inner={"text": dy.String(), "language": dy.String(nullable=True)}),
        nullable=True,
        alias="alert.recurrence_text.translation",
    )
    active_period = dy.List(
        dy.Struct(inner={"start": dy.UInt64(nullable=True), "end": dy.UInt64(nullable=True)}),
        nullable=True,
        alias="alert.active_period",
    )
    informed_entity = dy.List(
        dy.Struct(
            inner={
                "agency_id": dy.String(nullable=True),
                "route_id": dy.String(nullable=True),
                "route_type": dy.Int32(nullable=True),
                "direction_id": dy.UInt8(nullable=True),
                "trip": dy.Struct(
                    inner={
                        "trip_id": dy.String(nullable=True),
                        "route_id": dy.String(nullable=True),
                        "direction_id": dy.UInt8(nullable=True),
                        "start_time": dy.String(nullable=True),
                        "start_date": dy.String(nullable=True),
                        "schedule_relationship": dy.String(nullable=True),
                        "route_pattern_id": dy.String(nullable=True),
                        "tm_trip_id": dy.String(nullable=True),
                        "overload_id": dy.Int64(nullable=True),
                        "overload_offset": dy.Int64(nullable=True),
                        "revenue": dy.Bool(nullable=True),
                        "last_trip": dy.Bool(nullable=True),
                    },
                    nullable=True,
                ),
                "stop_id": dy.String(nullable=True),
                "facility_id": dy.String(nullable=True),
                "activities": dy.List(dy.String(), nullable=True),
            }
        ),
        nullable=True,
        alias="alert.informed_entity",
    )
    reminder_times = dy.List(dy.UInt64(), nullable=True, alias="alert.reminder_times")


class RtAlertsDetail(GTFSRTDetail):
    """How to convert RT GTFS Alerts from structs into a table."""

    record_schema = RtAlertMessage
    table_schema = RtAlertTable
    remote_location = rt_alerts
