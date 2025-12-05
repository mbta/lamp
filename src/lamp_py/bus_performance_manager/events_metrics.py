from typing import List, Tuple
from datetime import date

import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_joined import TMDailyWorkPiece
from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule
from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm import generate_tm_events, get_daily_work_pieces
from lamp_py.bus_performance_manager.events_joined import BusEvents, join_rt_to_schedule
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule
from lamp_py.runtime_utils.process_logger import ProcessLogger


class BusPerformanceMetrics(BusEvents):  # pylint: disable=too-many-ancestors
    "Bus events enriched with derived operational metrics."
    gtfs_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    previous_stop_id = dy.String(nullable=True)
    stop_arrival_dt = dy.Datetime(nullable=True, time_zone="UTC")
    stop_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_first_in_transit_seconds = dy.Int64(nullable=True)
    stop_arrival_seconds = dy.Int64(nullable=True)
    stop_departure_seconds = dy.Int64(nullable=True)
    travel_time_seconds = dy.Int64(nullable=True)
    stopped_duration_seconds = dy.Int64(nullable=True)
    route_direction_headway_seconds = dy.Int64(nullable=True, min=0)
    direction_destination_headway_seconds = dy.Int64(nullable=True, min=0)
    is_full_trip = dy.Bool(nullable=True)

    @dy.rule()
    def departure_after_arrival() -> pl.Expr:  # pylint: disable=no-method-argument
        "stop_departure_dt always follows stop_arrival_dt (when both are not null)."
        return pl.coalesce(pl.col("stop_arrival_dt") <= pl.col("stop_departure_dt"), pl.lit(True))

    @dy.rule()
    def stop_sequence_implies_arrival_order() -> pl.Expr:  # pylint: disable=no-method-argument
        "Stop arrival increases monotonically with stop sequence."
        return pl.col("stop_arrival_dt").ge(  # dt for current stop greater or equal than
            pl.col("stop_arrival_dt")  # dt for
            .shift(1)  # last stop
            .over(
                partition_by=["service_date", "trip_id", "vehicle_label"],
                order_by="stop_sequence",
            )
        )

    @dy.rule()
    def stop_sequence_implies_departure_order() -> pl.Expr:  # pylint: disable=no-method-argument
        "Stop departure increase monotonically with stop sequence."
        return pl.col("stop_departure_dt").ge(  # dt for current stop  # greater than
            pl.col("stop_departure_dt")  # dt for last stop
            .shift(1)
            .over(
                partition_by=["service_date", "trip_id", "vehicle_label"],
                order_by="stop_sequence",
            )
        )

    @dy.rule(group_by=["service_date", "trip_id", "vehicle_label"])
    def travel_time_plus_stopped_duration_equals_total_trip() -> pl.Expr:  # pylint: disable=no-method-argument
        "Summing the travel times and the stopped duration is equivalent to the total time from first arrival/departure to last arrival."
        return (
            pl.sum_horizontal("travel_time_seconds", "stopped_duration_seconds")
            .sum()
            .eq(
                (
                    pl.coalesce("stop_departure_dt", "stop_arrival_dt").max()
                    - pl.coalesce("stop_arrival_dt", "stop_departure_dt").min()
                ).dt.total_seconds()
            )
        )


def run_bus_performance_pipeline(
    service_date: date,
    gtfs_files: List[str],
    tm_files: List[str],
    tm_files_daily_work_pieces: List[str],
    **debug_flags: dict[str, bool],
) -> Tuple[dy.DataFrame[BusPerformanceMetrics], dy.DataFrame[TMDailyWorkPiece]]:
    """
    create dataframe of Bus Performance metrics to write to S3

    :param service_date: date of service being processed
    :param gtfs_files: list of RT_VEHCILE_POSITION parquet file paths, from S3, that cover service date
    :param tm_files: list of TM/STOP_CROSSING parquet file paths, from S3, that cover service date
    :param tm_files_daily_work_pieces: list of TM/DAILY_WORK_PIECE parquet file paths, from S3, that cover service date
    :param debug_flags: flags for debug purposes only! controlling writing of output files to local or s3.

    :return BusEvents:
    """
    # gtfs-rt events from parquet

    # _1, _2
    gtfs_schedule = bus_gtfs_schedule_events_for_date(service_date)

    # no OL, no _1, _2 trips??
    tm_schedule = generate_tm_schedule(service_date)

    # full join results in _1, _2, all TM, all GTFS
    combined_schedule = join_tm_schedule_to_gtfs_schedule(gtfs_schedule, tm_schedule.tm_schedule, **debug_flags)

    # _1, _2, -OL1, -OL2
    gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)
    # transit master events from parquet

    # _1, _2 without suffix, -OL without suffix.
    tm_df = generate_tm_events(tm_files, tm_schedule)

    if debug_flags.get("write_intermediates"):
        gtfs_schedule.write_parquet("/tmp/gtfs_schedule.parquet")
        gtfs_df.write_parquet("/tmp/gtfs_events.parquet")
        tm_df.write_parquet("/tmp/tm_events.parquet")
        combined_schedule.write_parquet("/tmp/combined_schedule.parquet")

    # get operator assignments
    tm_daily_work_pieces = get_daily_work_pieces(tm_files_daily_work_pieces)

    # create events dataframe with static schedule data, gtfs-rt events and transit master events
    bus_df = join_rt_to_schedule(combined_schedule, gtfs_df, tm_df, tm_daily_work_pieces)

    return calculate_derived_bus_performance_metrics(bus_df), tm_daily_work_pieces


def calculate_derived_bus_performance_metrics(
    bus_df: dy.DataFrame[BusEvents],
) -> dy.DataFrame[BusPerformanceMetrics]:
    """
    Derive new fields from the schedule and joined RT data.

    :param bus_df: pl.DataFrame returned by lamp_py.bus_performance_manager.events_joined.join_rt_to_schedule

    :return BusEvents:
    """
    process_logger = ProcessLogger("enrich_bus_performance_metrics")
    process_logger.log_start()

    enriched_bus_df = (
        bus_df.with_columns(
            pl.when(
                pl.col("point_type").eq(pl.lit("start")).any().over(partition_by=["trip_id", "vehicle_id"])
                & pl.col("point_type").eq(pl.lit("end")).any().over(partition_by=["trip_id", "vehicle_id"])
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("is_full_trip"),
            (  # for departure times
                pl.when(pl.col("stop_sequence").eq(pl.lit(1)))  # startpoints
                .then(pl.coalesce("gtfs_departure_dt", "tm_actual_departure_dt"))
                .otherwise(  # midpoints + endpoints
                    pl.min_horizontal(pl.col("tm_actual_departure_dt"), pl.col("gtfs_departure_dt")),
                )
            ).alias("stop_departure_dt"),
        )
        .with_columns(
            pl.min_horizontal(  # for arrival times
                pl.max_horizontal(pl.col("gtfs_arrival_dt"), pl.col("tm_actual_arrival_dt")),  # take the later
                pl.col("stop_departure_dt"),  # unless that conflicts with the departure time
            ).alias("stop_arrival_dt"),
            pl.col("stop_id")
            .shift(1)
            .over(
                ["vehicle_label", "trip_id"],
                order_by="stop_sequence",
            )
            .alias("previous_stop_id"),
        )
        .with_columns(
            pl.when(pl.col("stop_sequence").eq(pl.col("stop_count")))  # for endpoints
            .then(pl.col("stop_arrival_dt"))  # set departure equal to arrival
            .otherwise(pl.col("stop_departure_dt"))
            .alias("stop_departure_dt"),
        )
        .with_columns(
            *[  # force the stop_departure_dt & stop_arrival_dt in order with other stops in the trip by
                pl.when(pl.col(c).is_not_null())  # if the departure/arrival time isn't null
                .then(
                    pl.max_horizontal(  # take the max of
                        pl.col(c),  # the column
                        pl.max_horizontal("stop_departure_dt", "stop_arrival_dt", "gtfs_last_in_transit_dt")
                        .cum_max()  # the highest previous value of departure/arrival/travel_to dt
                        .fill_null(strategy="forward")
                        .shift()
                        .over(
                            partition_by=["service_date", "trip_id", "vehicle_label"],
                            order_by="stop_sequence",
                        ),
                    )
                )  # otherwise leave the departure/arrival time null
                .alias(c)
                for c in ["stop_departure_dt", "stop_arrival_dt"]
            ]
        )
        .with_columns(
            (pl.col("gtfs_first_in_transit_dt") - pl.col("service_date"))
            .dt.total_seconds()
            .alias("gtfs_first_in_transit_seconds"),
            (pl.col("stop_arrival_dt") - pl.col("service_date")).dt.total_seconds().alias("stop_arrival_seconds"),
            (pl.col("stop_departure_dt") - pl.col("service_date")).dt.total_seconds().alias("stop_departure_seconds"),
        )
        # add metrics columns to events
        .with_columns(
            (
                pl.coalesce(["stop_arrival_dt", "stop_departure_dt"])
                - pl.coalesce("stop_departure_dt", "stop_arrival_dt")
                .fill_null(strategy="forward")
                .shift()
                .over(partition_by=["service_date", "trip_id", "vehicle_label"], order_by="stop_sequence")
            )
            .dt.total_seconds()
            .alias("travel_time_seconds"),
            (pl.col("stop_departure_seconds") - pl.col("stop_arrival_seconds")).alias("stopped_duration_seconds"),
            (
                pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                - pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                .shift()
                .over(
                    ["service_date", "stop_id", "direction_id", "route_id"],
                    order_by=pl.coalesce(
                        "stop_departure_dt",
                        "stop_arrival_dt",
                        "gtfs_last_in_transit_dt",
                    ),
                )
            ).alias("route_direction_headway_seconds"),
            (
                pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                - (
                    pl.coalesce(["stop_departure_seconds", "stop_arrival_seconds"])
                    .shift()
                    .over(
                        ["service_date", "stop_id", "direction_destination"],
                        order_by=pl.coalesce(
                            "stop_departure_dt",
                            "stop_arrival_dt",
                            "gtfs_last_in_transit_dt",
                        ),
                    )
                )
            ).alias("direction_destination_headway_seconds"),
        )
        # sort to reduce parquet file size
        .sort(["route_id", "vehicle_label", "stop_sequence"])
    )

    valid = process_logger.log_dataframely_filter_results(*BusPerformanceMetrics.filter(enriched_bus_df))

    process_logger.log_complete()

    return valid
