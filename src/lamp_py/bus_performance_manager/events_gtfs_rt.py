from datetime import date
from typing import List

import polars as pl

from lamp_py.bus_performance_manager.gtfs_utils import (
    bus_routes_for_service_date,
)


def read_vehicle_positions(
    service_date: date, gtfs_rt_files: List[str]
) -> pl.DataFrame:
    """
    Read gtfs realtime vehicle position files and pull out unique bus vehicle
    positions for a given service day.

    :param service_date: the service date to filter on
    :param gtfs_rt_files: a list of gtfs realtime files, either s3 urls or a
        local path

    :return dataframe:
        route_id -> String
        trip_id -> String
        stop_id -> String
        stop_sequence -> String
        direction_id -> Int8
        start_time -> String
        service_date -> String
        vehicle_id -> String
        vehicle_label -> String
        current_status -> String
        vehicle_timestamp -> Datetime
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
            & pl.col("vehicle.stop_id").is_not_null()
            & pl.col("vehicle.trip.trip_id").is_not_null()
            & pl.col("vehicle.vehicle.id").is_not_null()
            & pl.col("vehicle.timestamp").is_not_null()
            & pl.col("vehicle.trip.start_time").is_not_null()
        )
        .select(
            pl.col("vehicle.trip.route_id").cast(pl.String).alias("route_id"),
            pl.col("vehicle.trip.trip_id").cast(pl.String).alias("trip_id"),
            pl.col("vehicle.stop_id").cast(pl.String).alias("stop_id"),
            pl.col("vehicle.current_stop_sequence")
            .cast(pl.Int64)
            .alias("stop_sequence"),
            pl.col("vehicle.trip.direction_id")
            .cast(pl.Int8)
            .alias("direction_id"),
            pl.col("vehicle.trip.start_time")
            .cast(pl.String)
            .alias("start_time"),
            pl.col("vehicle.trip.start_date")
            .cast(pl.String)
            .alias("service_date"),
            pl.col("vehicle.vehicle.id").cast(pl.String).alias("vehicle_id"),
            pl.col("vehicle.vehicle.label")
            .cast(pl.String)
            .alias("vehicle_label"),
            pl.col("vehicle.current_status")
            .cast(pl.String)
            .alias("current_status"),
            pl.from_epoch("vehicle.timestamp").alias("vehicle_timestamp"),
        )
        .with_columns(
            pl.when(pl.col("current_status") == "INCOMING_AT")
            .then(pl.lit("IN_TRANSIT_TO"))
            .otherwise(pl.col("current_status"))
            .cast(pl.String)
            .alias("current_status"),
        )
        .sort(["vehicle_id", "vehicle_timestamp"])
        .collect()
    )

    return vehicle_positions


def positions_to_events(vehicle_positions: pl.DataFrame) -> pl.DataFrame:
    """
    using the vehicle positions dataframe, create a dataframe for each event by
    pivoting and mapping the current status onto arrivals and departures.

    :param vehicle_positions: Dataframe of vehiclie positions

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> String
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> Int64
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime
        gtfs_arrival_dt -> Datetime
    """
    vehicle_events = vehicle_positions.pivot(
        values="vehicle_timestamp",
        aggregate_function="min",
        index=[
            "route_id",
            "direction_id",
            "trip_id",
            "stop_id",
            "stop_sequence",
            "start_time",
            "service_date",
            "vehicle_id",
            "vehicle_label",
        ],
        on="current_status",
    )

    for column in ["STOPPED_AT", "IN_TRANSIT_TO"]:
        if column not in vehicle_events.columns:
            vehicle_events = vehicle_events.with_columns(
                pl.lit(None).cast(pl.Datetime).alias(column)
            )

    vehicle_events = vehicle_events.rename(
        {
            "STOPPED_AT": "gtfs_arrival_dt",
            "IN_TRANSIT_TO": "gtfs_travel_to_dt",
        }
    ).select(
        [
            "service_date",
            "route_id",
            "trip_id",
            "start_time",
            "direction_id",
            "stop_id",
            "stop_sequence",
            "vehicle_id",
            "vehicle_label",
            "gtfs_travel_to_dt",
            "gtfs_arrival_dt",
        ]
    )

    return vehicle_events


def generate_gtfs_rt_events(
    service_date: date, gtfs_rt_files: List[str]
) -> pl.DataFrame:
    """
    generate a polars dataframe for bus vehicle events from gtfs realtime
    vehicle position files for a given service date

    :param service_date: the service date to filter on
    :param gtfs_rt_files: a list of gtfs realtime files, either s3 urls or a
        local path

    :return dataframe:
        route_id -> String
        trip_id -> String
        stop_id -> String
        stop_sequence -> String
        direction_id -> Int8
        start_time -> String
        service_date -> String
        vehicle_id -> String
        vehicle_label -> String
        current_status -> String
        arrival_gtfs -> Datetime
        travel_towards_gtfs -> Datetime
    """
    vehicle_positions = read_vehicle_positions(
        service_date=service_date, gtfs_rt_files=gtfs_rt_files
    )

    vehicle_events = positions_to_events(vehicle_positions=vehicle_positions)

    return vehicle_events
