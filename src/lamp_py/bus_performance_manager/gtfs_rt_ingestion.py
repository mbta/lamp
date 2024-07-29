from datetime import date
from typing import List

import polars as pl

from lamp_py.bus_performance_manager.gtfs_utils import bus_routes_for_service_date

def generate_gtfs_rt_events(
    service_date: date, gtfs_rt_files: List[str]
) -> pl.DataFrame:
    """
    generate a polars dataframe for bus vehicle events from gtfs realtime
    vehicle position files for a given service date
    """
    bus_routes = bus_routes_for_service_date(service_date)

    # build a dataframe of every gtfs record
    # * scan all of the gtfs realtime files
    # * only pull out bus records and records for the service date with current status fields
    # * rename / convert columns as appropriate
    # * convert 
    # * sort by vehicle id and timestamp
    # * keep only the first record for a given trip / stop / status
    vehicle_positions = (
        pl.scan_parquet(gtfs_rt_files)
        .filter(
            (pl.col("vehicle.trip.route_id").is_in(bus_routes))
            & (
                pl.col("vehicle.trip.start_date")
                == service_date.strftime("%Y%m%d")
            )
            & pl.col("vehicle.current_status").is_not_null()
        )
        .select(
            pl.col("vehicle.trip.route_id").cast(pl.String).alias("route_id"),
            pl.col("vehicle.trip.trip_id").cast(pl.String).alias("trip_id"),
            pl.col("vehicle.stop_id").cast(pl.String).alias("stop_id"),
            pl.col("vehicle.trip.direction_id").cast(pl.Int8).alias("direction_id"),
            pl.col("vehicle.trip.start_time").cast(pl.String).alias("start_time"),
            pl.col("vehicle.trip.start_date").cast(pl.String).alias("service_date"),
            pl.col("vehicle.vehicle.id").cast(pl.String).alias("vehicle_id"),
            pl.col("vehicle.current_status").cast(pl.String).alias('current_status'),
            pl.from_epoch("vehicle.timestamp").alias("vehicle_timestamp"),
        )
        .with_columns(
            pl.when(pl.col("current_status") == "INCOMING_AT").then(pl.lit("STOPPED_AT")).otherwise(pl.col("current_status")).cast(pl.String).alias("current_status"),
        )
        .sort(["vehicle_id", "vehicle_timestamp"])
        .unique(
            subset=[
                "route_id",
                "trip_id",
                "stop_id",
                "direction_id",
                "vehicle_id",
                "current_status",
            ],
            keep="first",
        )
        .collect()
    )

    # using the vehicle positions dataframe, create a dataframe for each event
    # by pivoting and mapping the current status onto arrivals and departures.
    # initially, they map to arrival times and traveling towards times. shift
    # the traveling towards datetime over trip id to get the departure time.
    vehicle_events = (
        vehicle_positions.pivot(
            values="vehicle_timestamp",
            index=[
                "route_id",
                "trip_id",
                "stop_id",
                "start_time",
                "service_date",
                "vehicle_id",
            ],
            columns="current_status",
        )
        .rename(
            {
                "STOPPED_AT": "arrival_gtfs",
                "IN_TRANSIT_TO": "travel_towards_gtfs",
            }
        )
    )

    return vehicle_events
