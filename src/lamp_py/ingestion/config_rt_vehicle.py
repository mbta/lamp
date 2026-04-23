from typing import List

import dataframely as dy
import msgspec
import polars as pl

from lamp_py.runtime_utils.remote_files import bus_vehicle_positions
from lamp_py.utils.typing import struct_to_schema
from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import FeedEntity, FeedMessage, GTFSRealtimeTable, VehiclePosition


class RtVehiclePositionEntity(FeedEntity, kw_only=True):
    """Each entity in the BusLoc VehiclePositions feed."""

    vehicle: VehiclePosition


class RtVehiclePositionMessage(FeedMessage):
    """A snapshot of the BusLoc VehiclePositions feed."""

    entity: List[RtVehiclePositionEntity]


class RtVehicleTable(GTFSRealtimeTable):
    """Flattened VehiclePositions data."""

    trip_id = dy.String(nullable=True, alias="vehicle.trip.trip_id")
    route_id = dy.String(nullable=True, alias="vehicle.trip.route_id")
    direction_id = dy.UInt8(nullable=True, alias="vehicle.trip.direction_id")
    start_time = dy.String(nullable=True, alias="vehicle.trip.start_time")
    start_date = dy.String(nullable=True, alias="vehicle.trip.start_date")
    revenue = dy.Bool(nullable=True, alias="vehicle.trip.revenue")
    vehicle_id = dy.String(nullable=True, alias="vehicle.vehicle.id")
    vehicle_label = dy.String(nullable=True, alias="vehicle.vehicle.label")
    timestamp = dy.Datetime(nullable=True, alias="vehicle.timestamp")
    stop_id = dy.String(nullable=True, alias="vehicle.stop_id")
    route_pattern_id = dy.String(nullable=True, alias="vehicle.trip.route_pattern_id")
    tm_trip_id = dy.String(nullable=True, alias="vehicle.trip.tm_trip_id")
    overload_id = dy.Int64(nullable=True, alias="vehicle.trip.overload_id")
    overload_offset = dy.Int64(nullable=True, alias="vehicle.trip.overload_offset")
    last_trip = dy.Bool(nullable=True, alias="vehicle.trip.last_trip")
    schedule_relationship = dy.String(nullable=True, alias="vehicle.trip.schedule_relationship")
    license_plate = dy.String(nullable=True, alias="vehicle.vehicle.license_plate")
    assignment_status = dy.String(nullable=True, alias="vehicle.vehicle.assignment_status")
    bearing = dy.UInt16(nullable=True, alias="vehicle.position.bearing")
    latitude = dy.Float64(nullable=True, alias="vehicle.position.latitude")
    longitude = dy.Float64(nullable=True, alias="vehicle.position.longitude")
    speed = dy.Float64(nullable=True, alias="vehicle.position.speed")
    odometer = dy.Float64(nullable=True, alias="vehicle.position.odometer")
    current_stop_sequence = dy.UInt32(nullable=True, alias="vehicle.current_stop_sequence")
    congestion_level = dy.String(nullable=True, alias="vehicle.congestion_level")
    occupancy_status = dy.String(nullable=True, alias="vehicle.occupancy_status")
    occupancy_percentage = dy.UInt32(nullable=True, alias="vehicle.occupancy_percentage")
    current_status = dy.String(nullable=True, alias="vehicle.current_status")


class RtVehicleDetail(GTFSRTDetail):
    """How to convert BusLoc Vehicle Positions from structs into a table."""

    record_schema = RtVehiclePositionMessage
    table_schema = RtVehicleTable
    remote_location = bus_vehicle_positions

    def transform_for_write(self, records: List[FeedMessage]) -> dy.LazyFrame[RtVehicleTable]:
        """Flatten BusLoc VehiclePositions messages."""
        jsons = msgspec.json.encode(records)
        lf = (
            pl.read_json(jsons, schema=struct_to_schema(self.record_schema).to_polars_schema())
            .lazy()
            .select("entity", pl.col("header").struct.field("timestamp").alias("feed_timestamp"))
            .explode("entity")
            .unnest()
            .unnest(separator=".")
            .unnest(separator=".")
        )
        valid = self.table_schema.validate(lf, eager=False, cast=True)

        return valid
