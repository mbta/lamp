from typing import List, Sequence
from enum import IntEnum

import dataframely as dy
import msgspec


class FeedHeader(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-feedheader"""

    gtfs_realtime_version: str
    incrementality: str
    timestamp: int
    feed_version: str | None = None


class FeedEntity(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-feedentity"""

    id: str
    is_deleted: bool | None = None


class FeedMessage(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-feedmessage"""

    header: FeedHeader
    entity: Sequence[FeedEntity]


class Position(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-position"""

    latitude: float
    longitude: float
    bearing: int | None = None
    speed: float | None = None
    odometer: float | None = None


class DirectionId(IntEnum):
    """Direction ID as defined in GTFS spec (0 or 1)"""

    OUTBOUND = 0
    INBOUND = 1


class TripDescriptor(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-tripdescriptor"""

    trip_id: str | None = None
    route_id: str | None = None
    direction_id: DirectionId | None = None
    start_time: str | None = None
    start_date: str | None = None
    schedule_relationship: str | None = None
    route_pattern_id: str | None = None  # MBTA Enhanced Field
    tm_trip_id: str | None = None  # Only used by Busloc
    overload_id: int | None = None  # Only used by Busloc
    overload_offset: int | None = None  # Only used by Busloc
    revenue: bool | None = None  # MBTA Enhanced Field
    last_trip: bool | None = None  # MBTA Enhanced Field


class ConsistElement(msgspec.Struct):
    """Vehicle consist element"""

    label: str | None = None


class VehicleDescriptor(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-vehicledescriptor"""

    id: str
    label: str | None = None
    license_plate: str | None = None
    consist: list[ConsistElement] | None = None  # MBTA Enhanced Field
    assignment_status: str | None = None


class CarriageDetails(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-carriagedetails"""

    id: str | None = None
    label: str | None = None
    occupancy_status: str | None = None
    occupancy_percentage: int | None = None
    carriage_sequence: int | None = None


class VehiclePosition(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-vehicleposition"""

    position: Position
    timestamp: int
    trip: TripDescriptor
    vehicle: VehicleDescriptor
    current_stop_sequence: int | None = None
    stop_id: str | None = None
    congestion_level: str | None = None
    current_status: str | None = None
    occupancy_status: str | None = None
    occupancy_percentage: int | None = None
    multi_carriage_details: List[CarriageDetails] | None = None


class Operator(msgspec.Struct):
    """Vehicle operator information"""

    id: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    name: str | None = None
    logon_time: int | None = None


class Translation(msgspec.Struct):
    """Translation element"""

    text: str
    language: str | None = None


class TranslatedString(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-translatedstring"""

    translation: List[Translation]


class StopTimeEvent(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-stoptimeevent"""

    delay: int | None = None
    time: int | None = None
    uncertainty: int | None = None


class StopTimeUpdate(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-stoptimeupdate"""

    stop_sequence: int
    arrival: StopTimeEvent
    departure: StopTimeEvent
    stop_id: str | None = None
    schedule_relationship: str | None = None
    boarding_status: str | None = None  # MBTA Enhanced Field


class TripUpdate(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-tripupdate"""

    trip: TripDescriptor
    vehicle: VehicleDescriptor
    stop_time_update: List[StopTimeUpdate]
    timestamp: int | None = None
    delay: int | None = None


class TimeRange(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-timerange"""

    start: int | None = None
    end: int | None = None


class EntitySelector(msgspec.Struct):
    """https://gtfs.org/documentation/realtime/reference/#message-entityselector"""

    activities: List[str]
    agency_id: str | None = None
    route_id: str | None = None
    route_type: int | None = None
    direction_id: DirectionId | None = None
    trip: TripDescriptor | None = None
    stop_id: str | None = None
    facility_id: str | None = None  # MBTA Enhanced Field


class Alert(msgspec.Struct, kw_only=True):
    """https://gtfs.org/documentation/realtime/reference/#message-alert"""

    alert_lifecycle: str  # MBTA Enhanced Field
    last_modified_timestamp: int  # MBTA Enhanced Field
    header_text: TranslatedString
    description_text: TranslatedString | None = None
    informed_entity: List[EntitySelector]
    severity: int  # MBTA Enhanced Field
    service_effect_text: TranslatedString  # MBTA Enhanced Field
    active_period: List[TimeRange] | None = None
    cause: str | None = None
    cause_detail: str | None = None  # Spec says TranslatedString but MBTA uses string
    effect: str | None = None
    effect_detail: str | None = None  # Spec says TranslatedString but MBTA uses string
    url: TranslatedString | None = None
    severity_level: str | None = None
    created_timestamp: int | None = None  # MBTA Enhanced Field
    last_push_notification_timestamp: int | None = None  # MBTA Enhanced Field
    closed_timestamp: int | None = None  # MBTA Enhanced Field
    duration_certainty: str | None = None  # MBTA Enhanced Field
    reminder_times: List[int] | None = None  # MBTA Enhanced Field
    short_header_text: TranslatedString | None = None  # MBTA Enhanced Field
    timeframe_text: TranslatedString | None = None  # MBTA Enhanced Field
    recurrence_text: TranslatedString | None = None


class GTFSRealtimeTable(dy.Schema):
    """Base GTFS Realtime Table Schema"""

    id = dy.String(primary_key=True)
    feed_timestamp = dy.Int64(primary_key=True)
