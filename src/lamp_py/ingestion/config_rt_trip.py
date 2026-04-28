from typing import List

import dataframely as dy
import polars as pl

from lamp_py.runtime_utils.remote_files import springboard_rt_trip_updates

from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import FeedEntity, FeedMessage, GTFSRealtimeTable, TripUpdate


class RtTripUpdateEntity(FeedEntity, kw_only=True):
    """Each entity in the RT TripUpdates feed."""

    trip_update: TripUpdate


class RtTripUpdateMessage(FeedMessage):
    """A snapshot of the RT TripUpdates feed."""

    entity: List[RtTripUpdateEntity]


class RtTripTable(GTFSRealtimeTable):
    """Flattened RT TripUpdates data."""

    stop_sequence = dy.UInt32(primary_key=True, alias="trip_update.stop_time_update.stop_sequence")
    trip_id = dy.String(nullable=True, alias="trip_update.trip.trip_id")
    route_id = dy.String(nullable=True, alias="trip_update.trip.route_id")
    direction_id = dy.UInt8(nullable=True, alias="trip_update.trip.direction_id")
    start_time = dy.String(nullable=True, alias="trip_update.trip.start_time")
    start_date = dy.String(nullable=True, alias="trip_update.trip.start_date")
    schedule_relationship = dy.String(nullable=True, alias="trip_update.trip.schedule_relationship")
    route_pattern_id = dy.String(nullable=True, alias="trip_update.trip.route_pattern_id")
    tm_trip_id = dy.String(nullable=True, alias="trip_update.trip.tm_trip_id")
    overload_id = dy.Int64(nullable=True, alias="trip_update.trip.overload_id")
    overload_offset = dy.Int64(nullable=True, alias="trip_update.trip.overload_offset")
    revenue = dy.Bool(nullable=True, alias="trip_update.trip.revenue")
    last_trip = dy.Bool(nullable=True, alias="trip_update.trip.last_trip")
    vehicle_id = dy.String(nullable=True, alias="trip_update.vehicle.id")
    vehicle_label = dy.String(nullable=True, alias="trip_update.vehicle.label")
    vehicle_license_plate = dy.String(nullable=True, alias="trip_update.vehicle.license_plate")
    trip_update_timestamp = dy.UInt64(nullable=True, alias="trip_update.timestamp")
    trip_update_delay = dy.Int32(nullable=True, alias="trip_update.delay")
    stop_id = dy.String(nullable=True, alias="trip_update.stop_time_update.stop_id")
    arrival_delay = dy.Int32(nullable=True, alias="trip_update.stop_time_update.arrival.delay")
    arrival_time = dy.Int64(nullable=True, alias="trip_update.stop_time_update.arrival.time")
    arrival_uncertainty = dy.Int32(nullable=True, alias="trip_update.stop_time_update.arrival.uncertainty")
    departure_delay = dy.Int32(nullable=True, alias="trip_update.stop_time_update.departure.delay")
    departure_time = dy.Int64(nullable=True, alias="trip_update.stop_time_update.departure.time")
    departure_uncertainty = dy.Int32(nullable=True, alias="trip_update.stop_time_update.departure.uncertainty")
    stop_schedule_relationship = dy.String(nullable=True, alias="trip_update.stop_time_update.schedule_relationship")
    boarding_status = dy.String(nullable=True, alias="trip_update.stop_time_update.boarding_status")


class TripDetail(GTFSRTDetail):
    """How to convert RT GTFS Trip Updates from structs into a table."""

    def flatten_record(self, lf: pl.LazyFrame) -> dy.LazyFrame[GTFSRealtimeTable]:
        """Flatten the GTFS Realtime message depending on its type."""
        lf = (
            lf.explode("trip_update.stop_time_update")
            .unnest("trip_update.stop_time_update", separator=".")
            .unnest(separator=".")
        )
        valid = self.table_schema.validate(lf, eager=False, cast=True)

        return valid


class RtTripDetail(TripDetail):
    """How to convert RT GTFS Trip Updates from structs into a table."""

    record_schema = RtTripUpdateMessage
    table_schema = RtTripTable
    remote_location = springboard_rt_trip_updates
