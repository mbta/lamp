import sqlalchemy as sa
import polars as pl
from lamp_py.postgres.postgres_utils import DatabaseIndex, DatabaseManager
from lamp_py.postgres.rail_performance_manager_schema import TempEventCompare, VehicleTrips


temp_trips = (
    sa.select(
        TempEventCompare.pm_trip_id,
    )
    .distinct()
    .subquery()
)

rt_trips_summary_query = (
    sa.select(
        VehicleTrips.pm_trip_id,
        VehicleTrips.direction_id,
        sa.func.coalesce(VehicleTrips.branch_route_id, VehicleTrips.trunk_route_id).label("route_id"),
        VehicleTrips.start_time,
    )
    .select_from(VehicleTrips)
    .join(
        temp_trips,
        temp_trips.c.pm_trip_id == VehicleTrips.pm_trip_id,
    )
    .where(
        VehicleTrips.service_date == int(20250410),
        VehicleTrips.static_version_key == int(1744126668),
        VehicleTrips.first_last_station_match == sa.false(),
    )
)

df = pl.read_database(query=rt_trips_summary_query, connection=engine)

rpm_db_manager = DatabaseManager(db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER, verbose=True)
rpm_db_manager.execute(rt_trips_summary_query, disable_trip_tigger=True)
