from typing import List

import dataframely as dy

from lamp_py.runtime_utils.remote_files import bus_trip_updates

from .config_rt_trip import RtTripTable
from .gtfs_rt_detail import GTFSRTDetail
from .gtfs_rt_structs import FeedEntity, FeedMessage, Operator, TripUpdate


class BusTripUpdate(TripUpdate, kw_only=True):
    """Bus Trip Update with operator information."""

    operator: Operator


class BusTripUpdateEntity(FeedEntity, kw_only=True):
    """Each entity in the Bus TripUpdates feed."""

    trip_update: BusTripUpdate


class BusTripUpdateMessage(FeedMessage):
    """A snapshot of the Bus TripUpdates feed."""

    entity: List[BusTripUpdateEntity]


class BusTripTable(RtTripTable):
    """Flattened Bus TripUpdates data."""

    vehicle_consist = dy.List(
        dy.Struct(inner={"label": dy.String(nullable=True)}), nullable=True, alias="trip_update.vehicle.consist"
    )
    vehicle_assignment_status = dy.String(nullable=True, alias="trip_update.vehicle.assignment_status")
    operator_id = dy.String(nullable=True, alias="trip_update.operator.id")
    operator_first_name = dy.String(
        metadata={"reader_roles": ["OperatorName"]}, nullable=True, alias="trip_update.operator.first_name"
    )
    operator_last_name = dy.String(
        metadata={"reader_roles": ["OperatorName"]}, nullable=True, alias="trip_update.operator.last_name"
    )
    operator_name = dy.String(
        metadata={"reader_roles": ["OperatorName"]}, nullable=True, alias="trip_update.operator.name"
    )
    operator_logon_time = dy.UInt64(nullable=True, alias="trip_update.operator.logon_time")


class RtBusTripDetail(GTFSRTDetail):
    """How to convert Bus GTFS Trip Updates from structs into a table."""

    record_schema = BusTripUpdateMessage
    table_schema = BusTripTable
    remote_location = bus_trip_updates
