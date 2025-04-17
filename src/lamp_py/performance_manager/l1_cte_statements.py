from datetime import datetime
import os
import sqlalchemy as sa
import polars as pl
from sqlalchemy.sql.functions import rank
from lamp_py.postgres.rail_performance_manager_schema import (
    ServiceIdDates,
    StaticStops,
    StaticStopTimes,
    StaticTrips,
    VehicleEvents,
    VehicleTrips,
    StaticRoutes,
)

GTFS_ARCHIVE = "s3://mbta-performance/lamp/gtfs_archive"


def static_trips_subquery_pl(service_date: int) -> pl.DataFrame:
    """
    input:
        service_date: int - YYYYMMDD format service_date e.g. 20250410

    output:
        static_trips_subquery: pl.DataFrame of the joined result. column dtype match RDS db dtypes

    Polars implementation of static_trips_subquery in backup_rt_static_trip_match
    """
    service_year = str(service_date)[:4]
    static_prefix = os.path.join(GTFS_ARCHIVE, service_year)

    service_dt = datetime.strptime(str(service_date), "%Y%m%d")
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    calendar_lf = (
        pl.scan_parquet(f"{static_prefix}/calendar.parquet")
        .filter(
            pl.col("gtfs_active_date") <= service_date,
            pl.col("gtfs_end_date") >= service_date,
            pl.col("start_date") <= service_date,
            pl.col("end_date") >= service_date,
            pl.col(days[service_dt.weekday()]) == 1,
        )
        .select("service_id")
    )

    calendar_dates_lf = (
        pl.scan_parquet(f"{static_prefix}/calendar_dates.parquet")
        .filter(
            pl.col("gtfs_active_date") <= service_date,
            pl.col("gtfs_end_date") >= service_date,
            pl.col("date") == service_date,
        )
        .select("service_id", "exception_type")
    )

    service_ids = (
        calendar_lf.join(calendar_dates_lf.filter(pl.col("exception_type") == 2), on="service_id", how="anti")
        .join(
            calendar_dates_lf.filter(pl.col("exception_type") == 1),
            on="service_id",
            how="full",
            coalesce=True,
        )
        .drop("exception_type")
        .unique()
    )

    stop_times_lf = pl.scan_parquet(f"{static_prefix}/stop_times.parquet").filter(
        pl.col("gtfs_active_date") <= service_date,
        pl.col("gtfs_end_date") >= service_date,
    )
    trips_lf = pl.scan_parquet(f"{static_prefix}/trips.parquet").filter(
        pl.col("gtfs_active_date") <= service_date,
        pl.col("gtfs_end_date") >= service_date,
    )
    routes_lf = pl.scan_parquet(f"{static_prefix}/routes.parquet").filter(
        pl.col("gtfs_active_date") <= service_date,
        pl.col("gtfs_end_date") >= service_date,
        pl.col("route_type") != 3,
    )

    def get_red_branch(trip_stop_ids: set) -> str:
        """
        Return special redline branch names - helper
        """
        ashmont_stop_ids = {
            "70087",
            "70088",
            "70089",
            "70090",
            "70091",
            "70092",
            "70093",
            "70094",
            "70085",
            "70086",
        }
        if trip_stop_ids & ashmont_stop_ids:
            return "Red-A"
        return "Red-B"

    def apply_branch_route_id(series_list) -> str:  # type: ignore
        """
        Return special redline branch names
        """
        if str(series_list[0][0]) == "Red":
            return get_red_branch(set(series_list[1]))
        return series_list[0][0]

    # developer note: the casts are not necessary.
    #
    # static_trips columns are set to the same datatypes/schema as the database
    # representation for these columns this is not strictly necessary, as the call
    # to execute_with_data_pl() calls polars.to_dicts(), which converts everything
    # to native python types (Int16, Int32 --> Int) before updating the database.
    # We choose to maintain the correct types here to ensure overflows or other
    # dtype issues can be discovered in the python processing

    static_trips = (
        stop_times_lf.join(
            trips_lf.select("trip_id", "route_id", "service_id", "direction_id"),
            on="trip_id",
            how="inner",
            coalesce=True,
        )
        .join(routes_lf.select("route_id"), on="route_id", how="inner", coalesce=True)
        .join(service_ids, on="service_id", how="inner", coalesce=True)
        .select(
            pl.col("trip_id").alias("static_trip_id"),
            pl.col("route_id").alias("t_route_id"),
            pl.col("direction_id").cast(pl.Boolean),
            pl.col("arrival_time"),
            pl.col("stop_id"),
        )
        .group_by(["static_trip_id", "t_route_id", "direction_id"])
        .agg(
            pl.len().alias("static_stop_count"),
            pl.min("arrival_time").alias("static_start_time"),
            pl.map_groups(["t_route_id", "stop_id"], function=apply_branch_route_id, returns_scalar=True).alias(
                "route_id"
            ),
        )
        .drop("t_route_id")
        .collect()
    )

    static_trips = static_trips.with_columns(
        pl.col("static_start_time")
        .str.splitn(":", 3)
        .struct.rename_fields(["hour", "minute", "second"])
        .alias("fields")
    ).unnest("fields")

    static_trips = static_trips.with_columns(
        pl.duration(hours=pl.col("hour"), minutes=pl.col("minute"), seconds=pl.col("second"))
        .dt.total_seconds()
        .cast(pl.Int32)
        .alias("static_start_time"),
        pl.col("static_stop_count").cast(pl.Int16),
    ).drop(["hour", "minute", "second"])

    return static_trips


def static_trips_subquery(static_version_key: int, service_date: int) -> sa.sql.selectable.Subquery:
    """
    return Selectable representing all static trips on
    given service_date and static_version_key value combo

    created fields to be returned:
        - static_trip_first_stop (bool indicating first stop of trip)
        - static_trip_last_stop (bool indicating last stop of trip)
        - static_stop_rank (rank field counting from 1 to N number of stops on trip)
    """
    return (
        sa.select(
            StaticStopTimes.static_version_key,
            StaticStopTimes.trip_id.label("static_trip_id"),
            StaticStopTimes.arrival_time.label("static_stop_timestamp"),
            sa.func.coalesce(
                StaticStops.parent_station,
                StaticStops.stop_id,
            ).label("parent_station"),
            (
                sa.func.lag(StaticStopTimes.departure_time)
                .over(
                    partition_by=(
                        StaticStopTimes.static_version_key,
                        StaticStopTimes.trip_id,
                    ),
                    order_by=StaticStopTimes.stop_sequence,
                )
                .is_(None)
            ).label("static_trip_first_stop"),
            (
                sa.func.lead(StaticStopTimes.departure_time)
                .over(
                    partition_by=(
                        StaticStopTimes.static_version_key,
                        StaticStopTimes.trip_id,
                    ),
                    order_by=StaticStopTimes.stop_sequence,
                )
                .is_(None)
            ).label("static_trip_last_stop"),
            rank()
            .over(
                partition_by=(
                    StaticStopTimes.static_version_key,
                    StaticStopTimes.trip_id,
                ),
                order_by=StaticStopTimes.stop_sequence,
            )
            .label("static_trip_stop_rank"),
            StaticTrips.route_id,
            StaticTrips.branch_route_id,
            StaticTrips.trunk_route_id,
            StaticTrips.direction_id,
        )
        .select_from(StaticStopTimes)
        .join(
            StaticTrips,
            sa.and_(
                StaticStopTimes.static_version_key == StaticTrips.static_version_key,
                StaticStopTimes.trip_id == StaticTrips.trip_id,
            ),
        )
        .join(
            StaticStops,
            sa.and_(
                StaticStopTimes.static_version_key == StaticStops.static_version_key,
                StaticStopTimes.stop_id == StaticStops.stop_id,
            ),
        )
        .join(
            ServiceIdDates,
            sa.and_(
                StaticStopTimes.static_version_key == ServiceIdDates.static_version_key,
                StaticTrips.service_id == ServiceIdDates.service_id,
                StaticTrips.route_id == ServiceIdDates.route_id,
            ),
        )
        .join(
            StaticRoutes,
            sa.and_(
                StaticStopTimes.static_version_key == StaticRoutes.static_version_key,
                StaticTrips.route_id == StaticRoutes.route_id,
            ),
        )
        .where(
            StaticStopTimes.static_version_key == int(static_version_key),
            StaticTrips.static_version_key == int(static_version_key),
            StaticStops.static_version_key == int(static_version_key),
            ServiceIdDates.static_version_key == int(static_version_key),
            StaticRoutes.static_version_key == int(static_version_key),
            StaticRoutes.route_type != 3,
            ServiceIdDates.service_date == int(service_date),
        )
        .subquery(name="static_trips_sub")
    )


def rt_trips_subquery(service_date: int) -> sa.sql.selectable.Subquery:
    """
    return Selectable representing all RT trips on a given service date

    created fields to be returned:
        - rt_trip_first_stop_flag (bool indicating first stop of trip by trip_hash)
        - rt_trip_last_stop_flag (bool indicating last stop of trip by trip_hash)
        - static_stop_rank (rank field counting from 1 to N number of stops on trip by trip_hash)
    """

    return (
        sa.select(
            VehicleTrips.static_version_key,
            VehicleTrips.direction_id,
            VehicleTrips.route_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
            VehicleTrips.service_date,
            VehicleTrips.start_time,
            VehicleTrips.vehicle_id,
            VehicleTrips.stop_count,
            VehicleTrips.static_trip_id_guess,
            VehicleTrips.revenue,
            VehicleEvents.pm_trip_id,
            VehicleEvents.stop_sequence,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp,
            VehicleEvents.vp_stop_timestamp,
            VehicleEvents.tu_stop_timestamp,
            (
                sa.func.lag(VehicleEvents.pm_trip_id)
                .over(
                    partition_by=(VehicleEvents.pm_trip_id),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_first_stop_flag"),
            (
                sa.func.lead(VehicleEvents.pm_trip_id)
                .over(
                    partition_by=(VehicleEvents.pm_trip_id),
                    order_by=VehicleEvents.stop_sequence,
                )
                .is_(None)
            ).label("rt_trip_last_stop_flag"),
            rank()
            .over(
                partition_by=(VehicleEvents.pm_trip_id),
                order_by=VehicleEvents.stop_sequence,
            )
            .label("rt_trip_stop_rank"),
        )
        .select_from(VehicleEvents)
        .join(
            VehicleTrips,
            VehicleTrips.pm_trip_id == VehicleEvents.pm_trip_id,
        )
        .where(
            VehicleTrips.service_date == service_date,
            VehicleEvents.service_date == service_date,
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
    ).subquery(name="rt_trips_sub")


def trips_for_metrics_subquery(static_version_key: int, service_date: int) -> sa.sql.selectable.Subquery:
    """
    return Selectable named "trips_for_metrics" with fields needed to develop metrics tables

    will return one record for every unique trip-stop on 'service_date'

    joins rt_trips_sub to static_trips_sub on static_trip_id_guess, static_version_key, parent_station and static_stop_rank,

    the join with static_stop_rank is required for routes that may visit the same
    parent station more than once on the same route, I think this only occurs on
    bus routes, so we may be able to drop this for performance_manager
    """

    static_trips_sub = static_trips_subquery(static_version_key, service_date)
    rt_trips_sub = rt_trips_subquery(service_date)

    return (
        sa.select(
            rt_trips_sub.c.static_version_key,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.direction_id,
            rt_trips_sub.c.route_id,
            rt_trips_sub.c.branch_route_id,
            rt_trips_sub.c.trunk_route_id,
            rt_trips_sub.c.stop_count,
            rt_trips_sub.c.start_time,
            rt_trips_sub.c.vehicle_id,
            rt_trips_sub.c.parent_station,
            rt_trips_sub.c.vp_move_timestamp.label("move_timestamp"),
            sa.func.coalesce(
                rt_trips_sub.c.vp_stop_timestamp,
                rt_trips_sub.c.tu_stop_timestamp,
            ).label("stop_timestamp"),
            sa.func.coalesce(
                rt_trips_sub.c.vp_move_timestamp,
                rt_trips_sub.c.vp_stop_timestamp,
                rt_trips_sub.c.tu_stop_timestamp,
            ).label("sort_timestamp"),
            sa.func.coalesce(
                static_trips_sub.c.static_trip_first_stop,
                rt_trips_sub.c.rt_trip_first_stop_flag,
            ).label("first_stop_flag"),
            sa.func.coalesce(
                static_trips_sub.c.static_trip_last_stop,
                rt_trips_sub.c.rt_trip_last_stop_flag,
            ).label("last_stop_flag"),
        )
        .distinct(
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
        .select_from(rt_trips_sub)
        .join(
            static_trips_sub,
            sa.and_(
                rt_trips_sub.c.static_trip_id_guess == static_trips_sub.c.static_trip_id,
                rt_trips_sub.c.static_version_key == static_trips_sub.c.static_version_key,
                rt_trips_sub.c.parent_station == static_trips_sub.c.parent_station,
                rt_trips_sub.c.rt_trip_stop_rank >= static_trips_sub.c.static_trip_stop_rank,
            ),
            isouter=True,
        )
        .order_by(
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
            static_trips_sub.c.static_trip_stop_rank,
        )
    ).subquery(name="trip_for_metrics")


def trips_for_headways_subquery(
    service_date: int,
) -> sa.sql.selectable.Subquery:
    """
    return Selectable named "trip_for_headways" with fields needed to develop headways values

    will return one record for every unique trip-stop on 'service_date'
    """

    rt_trips_sub = rt_trips_subquery(service_date)

    return (
        sa.select(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.service_date,
            rt_trips_sub.c.direction_id,
            rt_trips_sub.c.route_id,
            rt_trips_sub.c.branch_route_id,
            rt_trips_sub.c.trunk_route_id,
            rt_trips_sub.c.parent_station,
            rt_trips_sub.c.stop_count,
            rt_trips_sub.c.vehicle_id,
            rt_trips_sub.c.vp_move_timestamp.label("move_timestamp"),
            sa.func.lead(rt_trips_sub.c.vp_move_timestamp)
            .over(
                partition_by=rt_trips_sub.c.pm_trip_id,
                order_by=sa.func.coalesce(
                    rt_trips_sub.c.vp_move_timestamp,
                    rt_trips_sub.c.vp_stop_timestamp,
                ),
            )
            .label("next_station_move"),
        )
        .distinct(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
        .select_from(rt_trips_sub)
        .where(
            # drop trips with one stop count, probably not valid
            rt_trips_sub.c.stop_count > 1,
            rt_trips_sub.c.revenue == sa.true(),
        )
        .order_by(
            rt_trips_sub.c.pm_trip_id,
            rt_trips_sub.c.parent_station,
        )
    ).subquery(name="trip_for_headways")
