from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

SqlBase: Any = declarative_base()


class StaticSubHeadway(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for Static Subway Headway information"""

    __tablename__ = "staticSubwayHeadways"

    id = sa.Column(sa.Integer, primary_key=True)
    trip_id = sa.Column(sa.String(100))
    arrival_time = sa.Column(sa.String(10))
    departure_time = sa.Column(sa.String(10))
    stop_id = sa.Column(sa.Integer)
    stop_sequence = sa.Column(sa.Integer)
    pickup_type = sa.Column(sa.Integer)
    drop_off_type = sa.Column(sa.Integer)
    # TODO(zap) this should be an optional int, but it currently throws an
    # error if its a pandas.nan
    timepoint = sa.Column(sa.String)
    checkpoint_id = sa.Column(sa.String)
    route_type = sa.Column(sa.Integer)
    route_id = sa.Column(sa.String(60))
    service_id = sa.Column(sa.String(60))
    direction_id = sa.Column(sa.Integer)
    parent_station = sa.Column(sa.String(60))
    departure_time_sec = sa.Column(sa.Float)
    prev_departure_time_sec = sa.Column(sa.Float)
    head_way = sa.Column(sa.Integer)

    def __repr__(self) -> str:
        """this is just a helper string for debugging."""
        return f"( id='{self.id}', trip_id='{self.trip_id}', arrival_time='{self.arrival_time}', departure_time='{self.departure_time}', stop_id='{self.stop_id}', stop_sequence='{self.stop_sequence}', pickup_type='{self.pickup_type}', drop_off_type='{self.drop_off_type}', timepoint='{self.timepoint}', checkpoint_id='{self.checkpoint_id}', route_type='{self.route_type}', route_id='{self.route_id}', service_id='{self.service_id}', direction_id='{self.direction_id}', parent_station='{self.parent_station}', departure_time_sec='{self.departure_time_sec}', prev_departure_time_sec='{self.prev_departure_time_sec}', head_way='{self.head_way}')"


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
    hash = sa.Column(sa.BigInteger, nullable=False, index=True, unique=False)
    updated_on = sa.Column(
        sa.DateTime, server_default=sa.func.now(), server_onupdate=sa.func.now()
    )


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
    hash = sa.Column(sa.BigInteger, nullable=False, index=True, unique=False)
    updated_on = sa.Column(
        sa.DateTime, server_default=sa.func.now(), server_onupdate=sa.func.now()
    )


class MetadataLog(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for keeping track of parquet files in S3"""

    __tablename__ = "metadataLog"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    processed = sa.Column(sa.Boolean, default=sa.false())
    path = sa.Column(sa.String(256), nullable=False, unique=True)
    updated_on = sa.Column(
        sa.TIMESTAMP,
        server_default=sa.func.now(),
        server_onupdate=sa.func.now(),
    )
    created_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


class StaticFeedInfo(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS feed info"""

    __tablename__ = "staticFeedInfo"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    feed_start_date = sa.Column(sa.Integer, nullable=False)
    feed_end_date = sa.Column(sa.Integer, nullable=False)
    feed_version = sa.Column(sa.String(75), nullable=False, unique=True)
    timestamp = sa.Column(sa.Integer, nullable=False, unique=True)
    updated_on = sa.Column(
        sa.TIMESTAMP,
        server_default=sa.func.now(),
        server_onupdate=sa.func.now(),
    )
    created_on = sa.Column(sa.TIMESTAMP, server_default=sa.func.now())


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
    route_short_name = sa.Column(sa.String(60), nullable=False)
    route_long_name = sa.Column(sa.String(150), nullable=False)
    route_desc = sa.Column(sa.String(40), nullable=False)
    route_type = sa.Column(sa.SmallInteger, nullable=False)
    route_sort_order = sa.Column(sa.Integer, nullable=False)
    route_fare_class = sa.Column(sa.String(30), nullable=False)
    line_id = sa.Column(sa.String(30), nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticStops(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS stops"""

    __tablename__ = "staticStops"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    stop_id = sa.Column(sa.String(128), nullable=False)
    stop_name = sa.Column(sa.String(128), nullable=False)
    stop_desc = sa.Column(sa.String(256), nullable=False)
    platform_code = sa.Column(sa.String(10), nullable=False)
    platform_name = sa.Column(sa.String(60), nullable=False)
    parent_station = sa.Column(sa.String(30), nullable=False)
    timestamp = sa.Column(sa.Integer, nullable=False)


class StaticStopTimes(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for GTFS stop times"""

    __tablename__ = "staticStopTimes"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    trip_id = sa.Column(sa.String(128), nullable=False)
    arrival_time = sa.Column(sa.Integer, nullable=True)
    departure_time = sa.Column(sa.Integer, nullable=True)
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
