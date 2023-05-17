from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

SqlBase: Any = declarative_base()


class VehicleEvents(SqlBase):  # pylint: disable=too-few-public-methods
    """
    Table that hold GTFS-RT Events

    Store all of the information that identifies a trip and a stop. Then tie that
    information with timestamp events when a vehicle begins moring to the stop and
    when it arrived at the stop ()
    """

    __tablename__ = "vehicle_events"

    pk_id = sa.Column(sa.Integer, primary_key=True)

    # trip identifiers
    direction_id = sa.Column(sa.Boolean, nullable=False)
    route_id = sa.Column(sa.String(60), nullable=False)
    service_date = sa.Column(sa.Integer, nullable=False)
    start_time = sa.Column(sa.Integer, nullable=False)
    vehicle_id = sa.Column(sa.String(60), nullable=False)

    # hash of trip identifiers
    trip_hash = sa.Column(
        sa.LargeBinary(16), nullable=False, index=True, unique=False
    )

    # stop identifiers
    stop_sequence = sa.Column(sa.SmallInteger, nullable=False)
    stop_id = sa.Column(sa.String(60), nullable=False)
    parent_station = sa.Column(sa.String(60), nullable=False)

    # hash of trip and stop identifiers
    trip_stop_hash = sa.Column(
        sa.LargeBinary(16), nullable=False, index=True, unique=True
    )

    # event timestamps used for metrics
    vp_move_timestamp = sa.Column(sa.Integer, nullable=True)
    vp_stop_timestamp = sa.Column(sa.Integer, nullable=True)
    tu_stop_timestamp = sa.Column(sa.Integer, nullable=True)

    # forign key to static schedule expected values
    fk_static_timestamp = sa.Column(
        sa.Integer,
        sa.ForeignKey("static_feed_info.timestamp"),
        nullable=False,
    )

    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class VehicleTrips(SqlBase):  # pylint: disable=too-few-public-methods
    """
    Table that holds GTFS-RT Trips
    """

    __tablename__ = "vehicle_trips"

    trip_hash = sa.Column(sa.LargeBinary(16), nullable=False, primary_key=True)

    # trip identifiers
    direction_id = sa.Column(sa.Boolean, nullable=False)
    route_id = sa.Column(sa.String(60), nullable=False)
    branch_route_id = sa.Column(sa.String(60), nullable=True)
    trunk_route_id = sa.Column(sa.String(60), nullable=True)
    service_date = sa.Column(sa.Integer, nullable=False)
    start_time = sa.Column(sa.Integer, nullable=False)
    vehicle_id = sa.Column(sa.String(60), nullable=False)
    stop_count = sa.Column(sa.SmallInteger, nullable=True)

    trip_id = sa.Column(sa.String(128), nullable=True)
    vehicle_label = sa.Column(sa.String(128), nullable=True)
    vehicle_consist = sa.Column(sa.String(), nullable=True)
    direction = sa.Column(sa.String(30), nullable=True)
    direction_destination = sa.Column(sa.String(60), nullable=True)

    # static trip matching
    static_trip_id_guess = sa.Column(sa.String(128), nullable=True)
    static_start_time = sa.Column(sa.Integer, nullable=True)
    static_stop_count = sa.Column(sa.SmallInteger, nullable=True)
    first_last_station_match = sa.Column(
        sa.Boolean, nullable=False, default=sa.false()
    )

    # forign key to static schedule expected values
    fk_static_timestamp = sa.Column(
        sa.Integer,
        sa.ForeignKey("static_feed_info.timestamp"),
        nullable=False,
    )

    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class VehicleEventMetrics(SqlBase):  # pylint: disable=too-few-public-methods
    """
    Tables that holds pre-computed metrics for GTFS-RT Events
    """

    __tablename__ = "vehicle_event_metrics"

    trip_stop_hash = sa.Column(
        sa.LargeBinary(16), nullable=False, primary_key=True
    )

    travel_time_seconds = sa.Column(sa.Integer, nullable=True)
    dwell_time_seconds = sa.Column(sa.Integer, nullable=True)
    headway_trunk_seconds = sa.Column(sa.Integer, nullable=True)
    headway_branch_seconds = sa.Column(sa.Integer, nullable=True)

    updated_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class TempHashCompare(SqlBase):  # pylint: disable=too-few-public-methods
    """Hold temporary hash values for comparison to Vehicle Events table"""

    __tablename__ = "temp_hash_compare"

    hash = sa.Column(sa.LargeBinary(16), primary_key=True, nullable=False)


class MetadataLog(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for keeping track of parquet files in S3"""

    __tablename__ = "metadata_log"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    processed = sa.Column(sa.Boolean, default=sa.false())
    process_fail = sa.Column(sa.Boolean, default=sa.false())
    path = sa.Column(sa.String(256), nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


class StaticFeedInfo(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS feed info"""

    __tablename__ = "static_feed_info"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    feed_start_date = sa.Column(sa.Integer, nullable=False)
    feed_end_date = sa.Column(sa.Integer, nullable=False)
    feed_version = sa.Column(sa.String(75), nullable=False, unique=True)
    feed_active_date = sa.Column(sa.Integer, nullable=False, index=True)
    timestamp = sa.Column(sa.Integer, nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


class StaticTrips(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS trips"""

    __tablename__ = "static_trips"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    route_id = sa.Column(sa.String(60), nullable=False)
    branch_route_id = sa.Column(sa.String(60), nullable=True)
    trunk_route_id = sa.Column(sa.String(60), nullable=True)
    service_id = sa.Column(sa.String(60), nullable=False, index=True)
    trip_id = sa.Column(sa.String(128), nullable=False, index=True)
    direction_id = sa.Column(sa.Boolean, index=True)
    block_id = sa.Column(sa.String(128), nullable=True)
    timestamp = sa.Column(sa.Integer, nullable=False, index=True)


class StaticRoutes(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS routes"""

    __tablename__ = "static_routes"

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

    __tablename__ = "static_stops"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    stop_id = sa.Column(sa.String(128), nullable=False, index=True)
    stop_name = sa.Column(sa.String(128), nullable=False)
    stop_desc = sa.Column(sa.String(256), nullable=True)
    platform_code = sa.Column(sa.String(10), nullable=True)
    platform_name = sa.Column(sa.String(60), nullable=True)
    parent_station = sa.Column(sa.String(30), nullable=True)
    timestamp = sa.Column(sa.Integer, nullable=False, index=True)


class StaticStopTimes(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS stop times"""

    __tablename__ = "static_stop_times"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    trip_id = sa.Column(sa.String(128), nullable=False, index=True)
    arrival_time = sa.Column(sa.Integer, nullable=False)
    departure_time = sa.Column(sa.Integer, nullable=False)
    schedule_travel_time_seconds = sa.Column(sa.Integer, nullable=True)
    schedule_headway_trunk_seconds = sa.Column(sa.Integer, nullable=True)
    schedule_headway_branch_seconds = sa.Column(sa.Integer, nullable=True)
    stop_id = sa.Column(sa.String(30), nullable=False, index=True)
    stop_sequence = sa.Column(sa.SmallInteger, nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False, index=True)


class StaticCalendar(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS calendar"""

    __tablename__ = "static_calendar"

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


class StaticCalendarDates(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS Calendar Dates"""

    __tablename__ = "static_calendar_dates"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    service_id = sa.Column(sa.String(128), nullable=False, index=True)
    date = sa.Column(sa.Integer, nullable=False, index=True)
    exception_type = sa.Column(sa.SmallInteger, nullable=False)
    holiday_name = sa.Column(sa.String(128), nullable=True)
    timestamp = sa.Column(sa.Integer, nullable=False, index=True)


class StaticDirections(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS Calendar Dates"""

    __tablename__ = "static_directions"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    route_id = sa.Column(sa.String(60), nullable=False, index=True)
    direction_id = sa.Column(sa.Boolean, index=True)
    direction = sa.Column(sa.String(30), nullable=False)
    direction_destination = sa.Column(sa.String(60), nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False, index=True)


class TempStaticHeadwaysGen(SqlBase):  # pylint: disable=too-few-public-methods
    """
    Temp table for used for caulcating scheduled headways when a new static schedule is loaded
    added by migration de55dc40315d
    """

    __tablename__ = "temp_static_headways_gen"

    pk_id = sa.Column(sa.Integer, primary_key=True, autoincrement=False)
    departure_time = sa.Column(sa.Integer, nullable=False)
    parent_station = sa.Column(sa.String(30), nullable=False)
    service_id = sa.Column(sa.String(60), nullable=False)
    direction_id = sa.Column(sa.Boolean)
    trunk_route_id = sa.Column(sa.String(60), nullable=True)
    branch_route_id = sa.Column(sa.String(60), nullable=True)


class ServiceIdDates(SqlBase):  # pylint: disable=too-few-public-methods
    """
    Table representing service_id_by_date_and_route VIEW for use by performance_manager
    """

    __tablename__ = "static_service_id_lookup"

    dummy_pk = sa.Column(sa.Integer, primary_key=True)
    route_id = sa.Column(sa.String(60))
    service_id = sa.Column(sa.String(60))
    service_date = sa.Column(sa.Integer)
    timestamp = sa.Column(sa.Integer)
