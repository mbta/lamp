from datetime import date
from typing import List

import polars as pl
from pyarrow.fs import S3FileSystem
import pyarrow.compute as pc

from lamp_py.utils.gtfs_utils import bus_route_ids_for_service_date
from lamp_py.performance_manager.gtfs_utils import start_time_to_seconds
from lamp_py.runtime_utils.process_logger import ProcessLogger


def _read_with_polars(service_date: date, gtfs_rt_files: List[str], bus_routes: List[str]) -> pl.DataFrame:
    """
    Read RT_VEHICLE_POSITIONS parquet files with polars engine

    Polars engine appears to be faster and use less memory than pyarrow enginer, but is not as
    compatible with all parquet file formats as pyarrow engine
    """
    vehicle_positions = (
        pl.scan_parquet(gtfs_rt_files)
        .filter(
            (pl.col("vehicle.trip.route_id").is_in(bus_routes))
            & (pl.col("vehicle.trip.start_date") == service_date.strftime("%Y%m%d"))
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
            pl.col("vehicle.current_stop_sequence").cast(pl.Int64).alias("stop_sequence"),
            pl.col("vehicle.trip.direction_id").cast(pl.Int8).alias("direction_id"),
            pl.col("vehicle.trip.start_time").cast(pl.String).alias("start_time"),
            pl.col("vehicle.trip.start_date").cast(pl.String).alias("service_date"),
            pl.col("vehicle.vehicle.id").cast(pl.String).alias("vehicle_id"),
            pl.col("vehicle.vehicle.label").cast(pl.String).alias("vehicle_label"),
            pl.col("vehicle.current_status").cast(pl.String).alias("current_status"),
            pl.col("vehicle.position.latitude").cast(pl.Float64).alias("latitude"),
            pl.col("vehicle.position.longitude").cast(pl.Float64).alias("longitude"),
            pl.from_epoch("vehicle.timestamp").alias("vehicle_timestamp"),
        )
        # We only care if the bus is IN_TRANSIT_TO or STOPPED_AT, wso we're replacing the INCOMING_TO enum from this column
        # https://github.com/google/transit/blob/master/gtfs-realtime/spec/en/reference.md?plain=1#L270
        .with_columns(
            pl.when(pl.col("current_status") == "INCOMING_AT")
            .then(pl.lit("IN_TRANSIT_TO"))
            .otherwise(pl.col("current_status"))
            .cast(pl.String)
            .alias("current_status"),
        )
        .collect()
    )

    return vehicle_positions


def _read_with_pyarrow(service_date: date, gtfs_rt_files: List[str], bus_routes: List[str]) -> pl.DataFrame:
    """
    Read RT_VEHICLE_POSITIONS parquet files with pyarrow engine, instead of polars engine

    the polars implmentation of parquet reader sometimes has issues with files in staging bucket
    pyarrow engine is more forgiving in reading some parquet file formats at the cost of read speed
    and memory usage, compared to polars native parquet reader/scanner
    """
    gtfs_rt_files = [uri.replace("s3://", "") for uri in gtfs_rt_files]
    columns = [
        "vehicle.trip.route_id",
        "vehicle.trip.trip_id",
        "vehicle.trip.direction_id",
        "vehicle.trip.start_time",
        "vehicle.trip.start_date",
        "vehicle.vehicle.id",
        "vehicle.vehicle.label",
        "vehicle.stop_id",
        "vehicle.current_stop_sequence",
        "vehicle.current_status",
        "vehicle.timestamp",
        "vehicle.position.latitude",
        "vehicle.position.longitude",
    ]
    # pyarrow_exp filter expression is used to limit memory usage during read operation
    pyarrow_exp = pc.field("vehicle.trip.route_id").isin(bus_routes)
    vehicle_positions = (
        pl.read_parquet(
            gtfs_rt_files,
            columns=columns,
            use_pyarrow=True,
            pyarrow_options={"filesystem": S3FileSystem(), "filters": pyarrow_exp},
        )
        .filter(
            (pl.col("vehicle.trip.route_id").is_in(bus_routes))
            & (pl.col("vehicle.trip.start_date") == service_date.strftime("%Y%m%d"))
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
            pl.col("vehicle.current_stop_sequence").cast(pl.Int64).alias("stop_sequence"),
            pl.col("vehicle.trip.direction_id").cast(pl.Int8).alias("direction_id"),
            pl.col("vehicle.trip.start_time").cast(pl.String).alias("start_time"),
            pl.col("vehicle.trip.start_date").cast(pl.String).alias("service_date"),
            pl.col("vehicle.vehicle.id").cast(pl.String).alias("vehicle_id"),
            pl.col("vehicle.vehicle.label").cast(pl.String).alias("vehicle_label"),
            pl.col("vehicle.current_status").cast(pl.String).alias("current_status"),
            pl.col("vehicle.position.latitude").cast(pl.Float64).alias("latitude"),
            pl.col("vehicle.position.longitude").cast(pl.Float64).alias("longitude"),
            pl.from_epoch("vehicle.timestamp").alias("vehicle_timestamp"),
        )
        # We only care if the bus is IN_TRANSIT_TO or STOPPED_AT, wso we're replacing the INCOMING_TO enum from this column
        # https://github.com/google/transit/blob/master/gtfs-realtime/spec/en/reference.md?plain=1#L270
        .with_columns(
            pl.when(pl.col("current_status") == "INCOMING_AT")
            .then(pl.lit("IN_TRANSIT_TO"))
            .otherwise(pl.col("current_status"))
            .cast(pl.String)
            .alias("current_status"),
        )
    )

    return vehicle_positions


def read_vehicle_positions(service_date: date, gtfs_rt_files: List[str]) -> pl.DataFrame:
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
    logger = ProcessLogger(
        "read_vehicle_positions",
        service_date=service_date,
        file_count=len(gtfs_rt_files),
        reader_engine="polars",
    )
    logger.log_start()
    bus_routes = bus_route_ids_for_service_date(service_date)

    # need to investigate which is actually faster/works.
    # as of 7/15/25, the pyarrow reader was faster on my local machine
    try:
        vehicle_positions = _read_with_polars(service_date, gtfs_rt_files, bus_routes)
    except Exception as _:
        logger.add_metadata(reader_engine="pyarrow")
        vehicle_positions = _read_with_pyarrow(service_date, gtfs_rt_files, bus_routes)

    logger.log_complete()
    return vehicle_positions


def positions_to_events(vehicle_positions: pl.DataFrame) -> pl.DataFrame:
    """
    using the vehicle positions dataframe, create a row for each event by
    pivoting and mapping the current status onto arrivals and departures.

    :param vehicle_positions: Dataframe of vehicles positions

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> String
        start_dt -> Datetime
        stop_count -> UInt32
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> Int64
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime
        gtfs_arrival_dt -> Datetime
        latitude -> Float64
        longitude -> Float64
    """
    vehicle_positions = vehicle_positions.with_row_index()
    vehicle_events = vehicle_positions.pivot(
        values=["vehicle_timestamp"],
        # think on this - this min is grabbing the earliest values and labeling them "STOPPED_AT or IN_TRANSIT_TO"
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

    # this section adds in columns are for handling when the input dataframes are empty or if
    # the pivot does not successfully add in the values=[x] columns. they must be added
    # back in after the fact to maintain the expected interface further donwstream
    for column in ["STOPPED_AT", "IN_TRANSIT_TO"]:
        if column not in vehicle_events.columns:
            vehicle_events = vehicle_events.with_columns(pl.lit(None).cast(pl.Datetime).alias(column))

    # only grab the IN_TRANSIT_TO rows lat/lon because they seem to better
    # align to actual trips than STOPPED_AT does - caused by
    # vendor - details in linked Asana Ticket/PR #542
    event_position = (
        vehicle_positions.filter(pl.col("current_status") == "IN_TRANSIT_TO")
        .group_by("vehicle_id", "vehicle_timestamp")
        .agg(pl.col("latitude").first(), pl.col("longitude").first())
    )
    vehicle_events = vehicle_events.join(
        event_position,
        how="left",
        right_on=["vehicle_id", "vehicle_timestamp"],
        left_on=["vehicle_id", "IN_TRANSIT_TO"],
        coalesce=True,
        validate="m:1",
    )
    stop_count = vehicle_events.group_by("trip_id").len("stop_count")

    vehicle_events = (
        vehicle_events.join(
            stop_count,
            on="trip_id",
            how="left",
        )
        .rename(
            {
                "STOPPED_AT": "gtfs_arrival_dt",
                "IN_TRANSIT_TO": "gtfs_travel_to_dt",
            }
        )
        .with_columns(
            pl.col("gtfs_arrival_dt").dt.replace_time_zone("UTC", ambiguous="earliest"),
            pl.col("gtfs_travel_to_dt").dt.replace_time_zone("UTC", ambiguous="earliest"),
            (pl.col("start_time").map_elements(start_time_to_seconds, return_dtype=pl.Int64)),
            (pl.col("service_date").str.to_datetime("%Y%m%d") + pl.duration(seconds=pl.col("start_time"))).alias(
                "start_dt"
            ),
        )
        .select(
            [
                "service_date",
                "route_id",
                "trip_id",
                "start_time",
                "start_dt",
                "stop_count",
                "direction_id",
                "stop_id",
                "stop_sequence",
                "vehicle_id",
                "vehicle_label",
                "gtfs_travel_to_dt",
                "gtfs_arrival_dt",
                "latitude",
                "longitude",
            ]
        )
    )

    return vehicle_events


def generate_gtfs_rt_events(service_date: date, gtfs_rt_files: List[str]) -> pl.DataFrame:
    """
    generate a polars dataframe for bus vehicle events from gtfs realtime
    vehicle position files for a given service date

    :param service_date: the service date to filter on
    :param gtfs_rt_files: a list of gtfs realtime files, either s3 urls or a
        local path

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> Int64
        start_dt -> Datetime
        stop_count -> UInt32
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> Int64
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime
        gtfs_arrival_dt -> Datetime
    """
    logger = ProcessLogger("generate_gtfs_rt_events", service_date=service_date)
    logger.log_start()

    # if RT_VEHICLE_POSITIONS exists for whole day, filter out hour files for that day
    for year_file in [f for f in gtfs_rt_files if f.endswith("T00:00:00.parquet")]:
        prefix, _ = year_file.rsplit("/", 1)
        gtfs_rt_files = [f for f in gtfs_rt_files if f == year_file or not f.startswith(prefix)]

    vehicle_positions = read_vehicle_positions(service_date=service_date, gtfs_rt_files=gtfs_rt_files)
    logger.add_metadata(rows_from_parquet=vehicle_positions.shape[0])
    vehicle_events = positions_to_events(vehicle_positions=vehicle_positions)
    logger.add_metadata(events_for_day=vehicle_events.shape[0])

    logger.log_complete()
    return vehicle_events
