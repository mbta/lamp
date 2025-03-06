from typing import List
from datetime import date

import polars as pl

from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events
from lamp_py.bus_performance_manager.events_tm import generate_tm_events
from lamp_py.bus_performance_manager.events_joined import join_tm_to_rt
from lamp_py.bus_performance_manager.events_joined import join_schedule_to_rt


def bus_performance_metrics(service_date: date, gtfs_files: List[str], tm_files: List[str]) -> pl.DataFrame:
    """
    create dataframe of Bus Performance metrics to write to S3

    :param service_date: date of service being processed
    :param gtfs_files: list of RT_VEHCILE_POSITION parquet file paths, from S3, that cover service date
    :param tm_files: list of TM/STOP_CROSSING parquet file paths, from S3, that cover service date

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> Int64
        start_dt -> Datetime
        stop_count -> UInt32
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> String
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime
        gtfs_travel_to_seconds -> Int64
        stop_arrival_dt -> Datetime
        stop_arrival_seconds -> Int64
        stop_departure_dt -> Datetime
        stop_departure_seconds -> Int64
        tm_scheduled_time_dt -> Datetime
        tm_actual_departure_dt -> Datetime
        tm_actual_arrival_dt -> Datetime
        plan_trip_id -> String
        exact_plan_trip_match -> Bool
        block_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction -> String
        direction_destination -> String
        plan_stop_count -> UInt32
        plan_start_time -> Int64
        plan_start_dt -> Datetime
        stop_name -> String
        plan_travel_time_seconds -> Int64
        plan_route_direction_headway_seconds -> Int64
        plan_direction_destination_headway_seconds -> Int64
        travel_time_seconds -> Int64
        dwell_time_seconds -> Int64
        route_direction_headway_seconds -> Int64
        direction_destination_headway_seconds -> Int64
    """
    # gtfs-rt events from parquet
    gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)
    # transit master events from parquet
    tm_df = generate_tm_events(tm_files)
    # create events dataframe with static schedule data, gtfs-rt events and transit master events
    bus_df = join_schedule_to_rt(join_tm_to_rt(gtfs_df, tm_df))

    bus_df = (
        bus_df.with_columns(pl.coalesce(["gtfs_travel_to_dt", "gtfs_arrival_dt"]).alias("gtfs_sort_dt")).with_columns(
            (
                pl.col("gtfs_travel_to_dt")
                .shift(-1)
                .over(
                    ["vehicle_label", "trip_id"],
                    order_by="gtfs_sort_dt",
                )
            ).alias("gtfs_departure_dt"),
            # take the later of the two possible arrival times as the true arrival time
            (
                pl.when(pl.col("tm_actual_arrival_dt") > pl.col("gtfs_travel_to_dt"))
                .then(pl.col("tm_actual_arrival_dt"))
                .otherwise(pl.col("gtfs_arrival_dt"))
            ).alias("stop_arrival_dt"),
        )
        # take the later of the two possible departure times as the true departure time
        .with_columns(
            pl.when(pl.col("tm_actual_departure_dt") >= pl.col("stop_arrival_dt"))
            .then(pl.col("tm_actual_departure_dt"))
            .otherwise(pl.col("gtfs_departure_dt"))
            .alias("stop_departure_dt")
        )
        # convert dt columns to seconds after midnight
        .with_columns(
            (pl.col("gtfs_travel_to_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            .dt.total_seconds()
            .alias("gtfs_travel_to_seconds"),
            (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            .dt.total_seconds()
            .alias("stop_arrival_seconds"),
            (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
            .dt.total_seconds()
            .alias("stop_departure_seconds"),
        )
        # add metrics columns to events
        .with_columns(
            (pl.coalesce(["stop_arrival_seconds", "stop_departure_seconds"]) - pl.col("gtfs_travel_to_seconds")).alias(
                "travel_time_seconds"
            ),
            (pl.col("stop_departure_seconds") - pl.col("stop_arrival_seconds")).alias("dwell_time_seconds"),
            (
                pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                - pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                .shift()
                .over(
                    ["stop_id", "direction_id", "route_id"],
                    order_by="gtfs_sort_dt",
                )
            ).alias("route_direction_headway_seconds"),
            (
                pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                - (
                    pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                    .shift()
                    .over(
                        ["stop_id", "direction_destination"],
                        order_by="gtfs_sort_dt",
                    )
                )
            ).alias("direction_destination_headway_seconds"),
        )
        # sort to reduce parquet file size
        .sort(["route_id", "vehicle_label", "gtfs_sort_dt"])
        # drop temp fields and dev validation fields
        .drop(
            [
                "gtfs_departure_dt",
                "gtfs_arrival_dt",
                "gtfs_sort_dt",
            ]
        )
    )

    return bus_df
