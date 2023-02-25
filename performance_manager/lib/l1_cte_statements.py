import datetime
import calendar

import sqlalchemy as sa
from typing import Dict, List

from .postgres_schema import (
    VehicleEvents,
    VehicleTrips,
    VehicleEventMetrics,
    StaticCalendar,
    StaticStopTimes,
    StaticStops,
    StaticRoutes,
    StaticTrips
)

def get_static_trips_cte(timestamps: List[int], start_date: int) -> sa.sql.Selectable:
    """
    return CTE named "static_trip_cte" for all static trips on given date
    """
    start_date_dt = datetime.datetime.strptime(str(start_date), "%Y%m%d")
    day_of_week = calendar.day_name[start_date_dt.weekday()].lower()

    return (
        sa.select(
            StaticStopTimes.timestamp,
            StaticStopTimes.trip_id.label("static_trip_id"),
            StaticStopTimes.arrival_time.label("static_stop_timestamp"),
            sa.func.coalesce(
                StaticStops.parent_station,
                StaticStops.stop_id,
            ).label("parent_station"),
            (sa.func.lag(StaticStopTimes.departure_time).over(
                partition_by=(StaticStopTimes.timestamp,StaticStopTimes.trip_id),
                order_by=StaticStopTimes.stop_sequence,
            ) == None).label("static_trip_first_stop"),
            (sa.func.lead(StaticStopTimes.departure_time).over(
                partition_by=(StaticStopTimes.timestamp,StaticStopTimes.trip_id),
                order_by=StaticStopTimes.stop_sequence,
            ) == None).label("static_trip_last_stop"),
            sa.func.rank().over(
                partition_by=(StaticStopTimes.timestamp,StaticStopTimes.trip_id),
                order_by=StaticStopTimes.stop_sequence,
            ).label("static_trip_stop_rank"),
            StaticTrips.route_id,
            StaticTrips.direction_id,
        ).select_from(
            StaticStopTimes
        ).join(
            StaticTrips,
            sa.and_(
                StaticStopTimes.timestamp == StaticTrips.timestamp,
                StaticStopTimes.trip_id == StaticTrips.trip_id,
            ),
        ).join(
            StaticStops,
            sa.and_(
                StaticStopTimes.timestamp == StaticStops.timestamp,
                StaticStopTimes.stop_id == StaticStops.stop_id,
            ),
        ).join(
            StaticCalendar,
            sa.and_(
                StaticStopTimes.timestamp == StaticCalendar.timestamp,
                StaticTrips.service_id == StaticCalendar.service_id,
            ),
        ).where(
            (StaticStopTimes.timestamp.in_(timestamps))
            & (getattr(StaticCalendar, day_of_week) == sa.true())
            & (StaticCalendar.start_date <= int(start_date))
            & (StaticCalendar.end_date >= int(start_date))
        ).cte(name="static_trips_cte")
    )


def get_rt_trips_cte(start_date: int) -> sa.sql.Selectable:
    """
    return CTE named "rt_trips_cte" for all RT trips on a given date
    """

    return (
        sa.select(
            VehicleEvents.fk_static_timestamp,
            VehicleEvents.direction_id,
            VehicleEvents.route_id,
            VehicleEvents.start_date,
            VehicleEvents.start_time,
            VehicleEvents.vehicle_id,
            VehicleEvents.trip_hash,
            VehicleEvents.stop_sequence,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleEvents.tu_stop_timestamp,
            (sa.func.lag(VehicleEvents.trip_hash).over(
                partition_by=(VehicleEvents.trip_hash),
                order_by=VehicleEvents.stop_sequence,
            ) == None).label("rt_trip_first_stop_flag"),
            (sa.func.lead(VehicleEvents.trip_hash).over(
                partition_by=(VehicleEvents.trip_hash),
                order_by=VehicleEvents.stop_sequence,
            ) == None).label("rt_trip_last_stop_flag"),
            sa.func.rank().over(
                partition_by=(VehicleEvents.trip_hash),
                order_by=VehicleEvents.stop_sequence,
            ).label("rt_trip_stop_rank"),
        ).select_from(
            VehicleEvents
        ).where(
            VehicleEvents.start_date == start_date,
            sa.or_(
                VehicleEvents.vp_move_timestamp != None,
                VehicleEvents.vp_stop_timestamp != None,
            )
        )
    ).cte(name="rt_trips_cte")

