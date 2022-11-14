from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

SqlBase: Any = declarative_base()


class VehiclePositionEvents(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS-RT Vehicle Position Events"""

    __tablename__ = "eventsVehiclePositions"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    is_moving = sa.Column(sa.Boolean)
    stop_sequence = sa.Column(sa.SmallInteger, nullable=True)
    stop_id = sa.Column(sa.String(60), nullable=True)
    timestamp_start = sa.Column(sa.Integer, nullable=False)
    timestamp_end = sa.Column(sa.Integer, nullable=False)
    direction_id = sa.Column(sa.SmallInteger, nullable=True)
    route_id = sa.Column(sa.String(60), nullable=True)
    start_date = sa.Column(sa.Integer, nullable=True)
    start_time = sa.Column(sa.Integer, nullable=True)
    vehicle_id = sa.Column(sa.String(60), nullable=False)
    hash = sa.Column(
        sa.LargeBinary(16), nullable=False, index=True, unique=False
    )
    fk_static_timestamp = sa.Column(
        sa.Integer,
        sa.ForeignKey("staticFeedInfo.timestamp"),
        nullable=False,
    )
    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class TripUpdateEvents(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS-RT Trip Update Predicted Stop Events"""

    __tablename__ = "eventsTripUpdates"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    is_moving = sa.Column(sa.Boolean)
    stop_sequence = sa.Column(sa.SmallInteger, nullable=True)
    stop_id = sa.Column(sa.String(60), nullable=True)
    timestamp_start = sa.Column(sa.Integer, nullable=False)
    direction_id = sa.Column(sa.SmallInteger, nullable=True)
    route_id = sa.Column(sa.String(60), nullable=True)
    start_date = sa.Column(sa.Integer, nullable=True)
    start_time = sa.Column(sa.Integer, nullable=True)
    vehicle_id = sa.Column(sa.String(60), nullable=False)
    hash = sa.Column(
        sa.LargeBinary(16), nullable=False, index=True, unique=False
    )
    fk_static_timestamp = sa.Column(
        sa.Integer,
        sa.ForeignKey("staticFeedInfo.timestamp"),
        nullable=False,
    )
    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class MetadataLog(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for keeping track of parquet files in S3"""

    __tablename__ = "metadataLog"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    processed = sa.Column(sa.Boolean, default=sa.false())
    process_fail = sa.Column(sa.Boolean, default=sa.false())
    path = sa.Column(sa.String(256), nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


class StaticFeedInfo(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS feed info"""

    __tablename__ = "staticFeedInfo"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    feed_start_date = sa.Column(sa.Integer, nullable=False)
    feed_end_date = sa.Column(sa.Integer, nullable=False)
    feed_version = sa.Column(sa.String(75), nullable=False, unique=True)
    timestamp = sa.Column(sa.Integer, nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


class StaticTrips(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS trips"""

    __tablename__ = "staticTrips"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    route_id = sa.Column(sa.String(60), nullable=False)
    service_id = sa.Column(sa.String(60), nullable=False)
    trip_id = sa.Column(sa.String(128), nullable=False)
    direction_id = sa.Column(sa.Boolean)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticRoutes(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS routes"""

    __tablename__ = "staticRoutes"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    route_id = sa.Column(sa.String(60), nullable=False)
    agency_id = sa.Column(sa.SmallInteger, nullable=False)
    route_short_name = sa.Column(sa.String(60), nullable=True)
    route_long_name = sa.Column(sa.String(150), nullable=True)
    route_desc = sa.Column(sa.String(40), nullable=True)
    route_type = sa.Column(sa.SmallInteger, nullable=False)
    route_sort_order = sa.Column(sa.Integer, nullable=False)
    route_fare_class = sa.Column(sa.String(30), nullable=False)
    line_id = sa.Column(sa.String(30), nullable=True)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticStops(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS stops"""

    __tablename__ = "staticStops"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    stop_id = sa.Column(sa.String(128), nullable=False)
    stop_name = sa.Column(sa.String(128), nullable=False)
    stop_desc = sa.Column(sa.String(256), nullable=True)
    platform_code = sa.Column(sa.String(10), nullable=True)
    platform_name = sa.Column(sa.String(60), nullable=True)
    parent_station = sa.Column(sa.String(30), nullable=True)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticStopTimes(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS stop times"""

    __tablename__ = "staticStopTimes"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    trip_id = sa.Column(sa.String(128), nullable=False)
    arrival_time = sa.Column(sa.Integer, nullable=False)
    departure_time = sa.Column(sa.Integer, nullable=False)
    stop_id = sa.Column(sa.String(30), nullable=False)
    stop_sequence = sa.Column(sa.SmallInteger, nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticCalendar(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS calendar"""

    __tablename__ = "staticCalendar"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    service_id = sa.Column(sa.String(128), nullable=False)
    monday = sa.Column(sa.Boolean)
    tuesday = sa.Column(sa.Boolean)
    wednesday = sa.Column(sa.Boolean)
    thursday = sa.Column(sa.Boolean)
    friday = sa.Column(sa.Boolean)
    saturday = sa.Column(sa.Boolean)
    sunday = sa.Column(sa.Boolean)
    start_date = sa.Column(sa.Integer, nullable=False)
    end_date = sa.Column(sa.Integer, nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False)


class FullTripEvents(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for Level-1 GTFS-RT Trip Events"""

    __tablename__ = "fullTripEvents"

    hash = sa.Column(sa.LargeBinary(16), primary_key=True)
    fk_vp_moving_event = sa.Column(
        sa.Integer,
        sa.ForeignKey("eventsVehiclePositions.pk_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    fk_vp_stopped_event = sa.Column(
        sa.Integer,
        sa.ForeignKey("eventsVehiclePositions.pk_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    fk_tu_stopped_event = sa.Column(
        sa.Integer,
        sa.ForeignKey("eventsTripUpdates.pk_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class TempFullTripEvents(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for loading new Level-1 GTFS-RT Trip Events"""

    __tablename__ = "loadFullTripEvents"

    hash = sa.Column(sa.LargeBinary(16), primary_key=True)
    fk_vp_moving_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    fk_vp_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    fk_tu_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )


class TravelTimes(SqlBase):  # pylint: disable=too-few-public-methods
    """Level 2 Table for Travel Times"""

    __tablename__ = "travelTimes"

    # foreign key pointing to primary key in eventsVehiclePositions
    # for moving events
    fk_travel_time_id = sa.Column(
        sa.Integer,
        sa.ForeignKey("eventsVehiclePositions.pk_id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    travel_time_seconds = sa.Column(
        sa.Integer,
        nullable=False,
    )
    created_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class DwellTimes(SqlBase):  # pylint: disable=too-few-public-methods
    """Level 2 Table for Dwell Times"""

    __tablename__ = "dwellTimes"

    # foreign key pointing to primary key in eventsVehiclePositions
    # for stopped events
    fk_dwell_time_id = sa.Column(
        sa.Integer,
        sa.ForeignKey("eventsVehiclePositions.pk_id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    dwell_time_seconds = sa.Column(
        sa.Integer,
        nullable=False,
    )
    created_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class Headways(SqlBase):  # pylint: disable=too-few-public-methods
    """Level 2 Table for Headways"""

    __tablename__ = "headways"

    # foreign key pointing to primary key in fullTripEvents
    # for stopped events
    fk_trip_event_hash = sa.Column(
        sa.LargeBinary(16),
        sa.ForeignKey("fullTripEvents.hash", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    fk_vp_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    fk_tu_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    headway_seconds = sa.Column(
        sa.Integer,
        nullable=True,
    )
    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class TempHeadways(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for loading new Level-2 headways"""

    __tablename__ = "loadHeadways"

    fk_trip_event_hash = sa.Column(
        sa.LargeBinary(16),
        nullable=False,
        primary_key=True,
    )
    fk_vp_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    fk_tu_stopped_event = sa.Column(
        sa.Integer,
        nullable=True,
    )
    headway_seconds = sa.Column(
        sa.Integer,
        nullable=True,
    )
