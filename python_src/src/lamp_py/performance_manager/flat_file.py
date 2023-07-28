import sqlalchemy as sa
import pyarrow

from lamp_py.postgres.postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    StaticStopTimes,
)
from lamp_py.postgres.postgres_utils import DatabaseManager


def generate_daily_table(
    db_manager: DatabaseManager, service_date: int
) -> pyarrow.Table:
    """
    Generate a dataframe of all events and metrics for a single service date
    """
    query = (
        sa.select(
            VehicleEvents.stop_sequence,
            VehicleEvents.stop_id,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp.label("move_timestamp"),
            sa.func.coalesce(
                VehicleEvents.vp_stop_timestamp,
                VehicleEvents.tu_stop_timestamp,
            ).label("stop_timestamp"),
            VehicleEvents.travel_time_seconds,
            VehicleEvents.dwell_time_seconds,
            VehicleEvents.headway_trunk_seconds,
            VehicleEvents.headway_branch_seconds,
            VehicleTrips.service_date,
            VehicleTrips.route_id,
            VehicleTrips.direction_id,
            VehicleTrips.start_time,
            VehicleTrips.vehicle_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
            VehicleTrips.stop_count,
            VehicleTrips.trip_id,
            VehicleTrips.vehicle_label,
            VehicleTrips.vehicle_consist,
            VehicleTrips.direction,
            VehicleTrips.direction_destination,
            VehicleTrips.static_start_time,
            StaticStopTimes.arrival_time.label("scheduled_arrival_time"),
            StaticStopTimes.departure_time.label("scheduled_departure_time"),
            StaticStopTimes.schedule_travel_time_seconds.label(
                "scheduled_travel_time"
            ),
            StaticStopTimes.schedule_headway_branch_seconds.label(
                "scheduled_headway_branch"
            ),
            StaticStopTimes.schedule_headway_trunk_seconds.label(
                "scheduled_headway_trunk"
            ),
        )
        .join(VehicleTrips, VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id)
        .join(
            StaticStopTimes,
            sa.and_(
                StaticStopTimes.static_version_key
                == VehicleTrips.static_version_key,
                StaticStopTimes.trip_id == VehicleTrips.static_trip_id_guess,
                StaticStopTimes.stop_id == VehicleEvents.stop_id,
            ),
            isouter=True,
        )
        .where(
            sa.and_(
                VehicleEvents.service_date == service_date,
                sa.or_(
                    VehicleEvents.vp_move_timestamp.is_not(None),
                    VehicleEvents.vp_stop_timestamp.is_not(None),
                ),
            )
        )
    )

    # get the days events as a dataframe from postgres
    days_events = db_manager.select_as_dataframe(query)

    # transform the seru
    days_events["year"] = days_events.apply(
        lambda record: int(str(record["service_date"])[0:4]), axis=1
    )
    days_events["month"] = days_events.apply(
        lambda record: int(str(record["service_date"])[4:6]), axis=1
    )
    days_events["day"] = days_events.apply(
        lambda record: int(str(record["service_date"])[6:8]), axis=1
    )
    days_events.drop(columns="service_date")

    flat_schema = pyarrow.schema(
        [
            ("stop_sequence", pyarrow.int16()),
            ("stop_id", pyarrow.string()),
            ("parent_station", pyarrow.string()),
            ("move_timestamp", pyarrow.int64()),
            ("stop_timestamp", pyarrow.int64()),
            ("travel_time_seconds", pyarrow.int64()),
            ("dwell_time_seconds", pyarrow.int64()),
            ("headway_trunk_seconds", pyarrow.int64()),
            ("headway_branch_seconds", pyarrow.int64()),
            ("route_id", pyarrow.string()),
            ("direction_id", pyarrow.int8()),
            ("start_time", pyarrow.int64()),
            ("vehicle_id", pyarrow.string()),
            ("branch_route_id", pyarrow.string()),
            ("trunk_route_id", pyarrow.string()),
            ("stop_count", pyarrow.int16()),
            ("trip_id", pyarrow.string()),
            ("vehicle_label", pyarrow.string()),
            ("vehicle_consist", pyarrow.string()),
            ("direction", pyarrow.string()),
            ("direction_destination", pyarrow.string()),
            ("static_start_time", pyarrow.int64()),
            ("scheduled_arrival_time", pyarrow.int64()),
            ("scheduled_departure_time", pyarrow.int64()),
            ("scheduled_travel_time", pyarrow.int64()),
            ("scheduled_headway_branch", pyarrow.int64()),
            ("scheduled_headway_trunk", pyarrow.int64()),
            ("year", pyarrow.int16()),
            ("month", pyarrow.int8()),
            ("day", pyarrow.int8()),
        ]
    )

    return pyarrow.Table.from_pandas(days_events, schema=flat_schema)
