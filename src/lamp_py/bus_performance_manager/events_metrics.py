from typing import List
from datetime import date

import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule, CombinedSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events, GTFSEvents
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm import generate_tm_events, TransitMasterEvents
from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule


class BusEvents(CombinedSchedule, TransitMasterEvents, GTFSEvents):  # pylint: disable=too-many-ancestors
    "Stop events from GTFS-RT, TransitMaster, and GTFS Schedule."
    trip_id = dy.String(primary_key=True, nullable=False)
    stop_sequence = dy.Int64(nullable=True, primary_key=False)
    gtfs_sort_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    previous_stop_id = dy.String(nullable=True)
    stop_arrival_dt = dy.Datetime(nullable=True, time_zone="UTC")
    stop_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_travel_to_seconds = dy.Int64(nullable=True)
    stop_arrival_seconds = dy.Int64(nullable=True)
    stop_departure_seconds = dy.Int64(nullable=True)
    travel_time_seconds = dy.Int64(nullable=True)
    dwell_time_seconds = dy.Int64(nullable=True)
    route_direction_headway_seconds = dy.Int64(nullable=True)
    direction_destination_headway_seconds = dy.Int64(nullable=True)


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
        start_dt -> Datetime(time_unit='us', time_zone=None)
        stop_count -> UInt32
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> Int64
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime(time_unit='us', time_zone='UTC')
        gtfs_arrival_dt -> Datetime(time_unit='us', time_zone='UTC')
        latitude -> Float64
        longitude -> Float64
        tm_stop_sequence -> Int64
        timepoint_order -> UInt32
        tm_planned_sequence_start -> Int64
        tm_planned_sequence_end -> Int64
        timepoint_id -> Int64
        timepoint_abbr -> String
        timepoint_name -> String
        pattern_id -> Int64
        tm_scheduled_time_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_actual_arrival_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_actual_departure_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_scheduled_time_sam -> Int64
        tm_actual_arrival_time_sam -> Int64
        tm_actual_departure_time_sam -> Int64
        tm_point_type -> Int32
        is_full_trip -> Int32
        plan_trip_id -> String
        exact_plan_trip_match -> Boolean
        block_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction -> String
        direction_destination -> String
        plan_stop_count -> UInt32
        plan_start_time -> Int64
        plan_start_dt -> Datetime(time_unit='us', time_zone=None)
        plan_stop_departure_dt -> Datetime(time_unit='us', time_zone=None)
        stop_name -> String
        plan_travel_time_seconds -> Int64
        plan_route_direction_headway_seconds -> Int64
        plan_direction_destination_headway_seconds -> Int64
    """
    # gtfs-rt events from parquet

    gtfs_schedule = bus_gtfs_schedule_events_for_date(service_date)
    tm_schedule = generate_tm_schedule()
    combined_schedule = join_tm_schedule_to_gtfs_schedule(gtfs_schedule, tm_schedule)

    gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)

    # transit master events from parquet
    tm_df = generate_tm_events(tm_files, tm_schedule)

    # create events dataframe with static schedule data, gtfs-rt events and transit master events
    bus_df = join_rt_to_schedule(combined_schedule, gtfs_df, tm_df)

    return enrich_bus_performance_metrics(bus_df)


def enrich_bus_performance_metrics(bus_df: pl.DataFrame) -> dy.DataFrame[BusEvents]:
    """
    Derive new fields from the schedule and joined RT data.

    :param bus_df: pl.DataFrame returned by lamp_py.bus_performance_manager.events_joined.join_rt_to_schedule

    :return BusEvents:
    """
    bus_df = (
        bus_df.with_columns(
            pl.coalesce(["gtfs_travel_to_dt", "gtfs_arrival_dt"]).alias("gtfs_sort_dt"),
        ).with_columns(
            (
                pl.col("gtfs_travel_to_dt")
                .shift(-1)
                .over(
                    # this should technically include pattern_id as well,
                    # but the groups formed by [trip_id, pattern_id] == [trip_id]
                    ["vehicle_label", "trip_id"],
                    order_by="gtfs_sort_dt",
                )
            ).alias("gtfs_departure_dt"),
            (
                pl.col("stop_id")
                .shift(1)
                .over(
                    ["vehicle_label", "trip_id"],
                    order_by="gtfs_sort_dt",
                )
            ).alias("previous_stop_id"),
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
    )

    valid = BusEvents.validate(bus_df)

    return valid
