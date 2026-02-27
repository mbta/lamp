import os
from datetime import date

import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_gtfs_schedule import BusBaseSchema
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    tm_daily_sched_adherence_waiver_file,
    tm_geo_node_file,
    tm_route_file,
    tm_stop_crossing,
    tm_time_point_file,
    tm_trip_file,
    tm_vehicle_file,
)


class TransitMasterSchedule(BusBaseSchema):
    """TransitMaster scheduled events."""

    vehicle_label = dy.String(nullable=True)
    pattern_id = dy.Int64(nullable=True)
    tm_stop_sequence = dy.Int64(primary_key=True)
    timepoint_id = dy.Int64(nullable=True)
    timepoint_abbr = dy.String(nullable=True)
    timepoint_name = dy.String(nullable=True)
    tm_planned_sequence_end = dy.Int64(nullable=True)
    tm_planned_sequence_start = dy.Int64(nullable=True)
    service_date = dy.Date(nullable=True)
    tm_stop_departure_dt = dy.Datetime(nullable=False, time_zone=None)
    timepoint_order = dy.UInt32(nullable=True)
    waiver_remark = dy.String(nullable=True, regex=r"^([[:alpha:]]+|Unrecognized Code)$")
    STOP_CROSSING_ID = dy.Int64(nullable=False)
    PULLOUT_ID = dy.Int64(primary_key=True)


def generate_tm_schedule(service_date: date) -> dy.DataFrame[TransitMasterSchedule]:
    """

    Non service tiemepoints
    vs all timepoints that havent joined
    where avl is bad
    there are places where gtfs is bad at getting stop records
    one examples - silverline tunnel. gtfs rt - picks up on some stops, gtfs-rt is functionally not tracking through SL tunnel. those rows are basically not in lamp
    gtfs rt background
    start schedule - e
    drop any row without stuff at the end
    tm schedule - not for service stops - this will have those

    """
    # the geo node id is the transit master key and the geo node abbr is the
    # gtfs stop id
    logger = ProcessLogger("generate_tm_schedule")
    logger.log_start()
    tm_geo_nodes = (
        pl.scan_parquet(tm_geo_node_file.s3_uri)
        .select(
            "GEO_NODE_ID",
            "GEO_NODE_ABBR",
        )
        .filter(pl.col("GEO_NODE_ABBR").is_not_null())
        .unique()
    )

    # the route id is the transit master key and the route abbr is the gtfs
    # route id.
    # NOTE: some of these route ids have leading zeros
    tm_routes = (
        pl.scan_parquet(tm_route_file.s3_uri)
        .select(
            "ROUTE_ID",
            "ROUTE_ABBR",
        )
        .filter(pl.col("ROUTE_ABBR").is_not_null())
        .unique()
    )

    # the trip id is the transit master key and the trip serial number is the
    # gtfs trip id.
    tm_trips = (
        pl.scan_parquet(tm_trip_file.s3_uri)
        .select(
            "TRIP_ID",
            "TRIP_SERIAL_NUMBER",
            "Pattern_ID",
        )
        .rename({"Pattern_ID": "PATTERN_ID"})
        .filter(pl.col("TRIP_SERIAL_NUMBER").is_not_null() & pl.col("PATTERN_ID").is_not_null())
        .unique()
    )

    # the vehicle id is the transit master key and the property tag is the
    # vehicle label
    tm_vehicles = (
        pl.scan_parquet(tm_vehicle_file.s3_uri)
        .select(
            "VEHICLE_ID",
            "PROPERTY_TAG",
        )
        .filter(pl.col("PROPERTY_TAG").is_not_null())
        .unique()
    )

    tm_time_points = pl.scan_parquet(tm_time_point_file.s3_uri).select(
        "TIME_POINT_ID",
        "TIME_POINT_ABBR",
        "TIME_PT_NAME",
    )

    waivers = (
        pl.scan_parquet(tm_daily_sched_adherence_waiver_file.s3_uri)
        .filter(pl.col("MISSED_ALLOWED_FLAG").eq(pl.lit(1)))
        .select(
            "WAIVER_ID",
            pl.coalesce(
                pl.col("REMARK").str.extract(r"^([[:alpha:]]+)( )?[-:]").alias("waiver_remark"),
                pl.lit("Unrecognized Code"),
            ).alias("waiver_remark"),
        )  # remove freetext entries to avoid exposing operator IDs
    )

    stop_crossings = (
        pl.scan_parquet(os.path.join(tm_stop_crossing.s3_uri, f"1{service_date.strftime('%Y%m%d')}.parquet"))
        .filter(
            pl.col("ROUTE_ID").is_not_null(),
            pl.col("GEO_NODE_ID").is_not_null(),
            pl.col("TRIP_ID").is_not_null(),
            # by allowing null `VEHICLE_ID`, we get all scheduled and actual trips for the day
        )
        .join(waivers, on="WAIVER_ID", how="left")
        .join(tm_trips, on="TRIP_ID", how="left")
        .join(tm_geo_nodes, on="GEO_NODE_ID", how="left")
        .join(tm_time_points, on="TIME_POINT_ID", how="left")
        .join(tm_routes, on="ROUTE_ID", how="left")
        .join(tm_vehicles, on="VEHICLE_ID", how="left")
        .with_columns(
            (
                pl.col("CALENDAR_ID").cast(pl.String).str.to_date("1%Y%m%d").alias("service_date"),
                pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
                pl.col("GEO_NODE_ABBR").alias("stop_id"),
                pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
                pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
                pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
                pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
                pl.col("PATTERN_ID").cast(pl.Int64).alias("pattern_id"),
                (
                    pl.col("CALENDAR_ID").cast(pl.String).str.to_datetime("1%Y%m%d", time_unit="us")
                    + pl.duration(seconds=pl.col("SCHEDULED_TIME"))
                ).alias("tm_stop_departure_dt"),
                pl.col(["PATTERN_GEO_NODE_SEQ"]).rank(method="dense").over(["PATTERN_ID"]).alias("timepoint_order"),
                pl.col("ROUTE_ABBR").str.strip_chars_start(pl.lit("0")).alias("route_id"),
                pl.col("PATTERN_GEO_NODE_SEQ").max().over(["TRIP_SERIAL_NUMBER"]).alias("tm_planned_sequence_end"),
                pl.col("PATTERN_GEO_NODE_SEQ").min().over(["TRIP_SERIAL_NUMBER"]).alias("tm_planned_sequence_start"),
                pl.col("PROPERTY_TAG")
                .forward_fill()
                .backward_fill()
                .over(["PULLOUT_ID", "TRIP_SERIAL_NUMBER"], order_by="PATTERN_GEO_NODE_SEQ")
                .cast(pl.String)
                .alias("vehicle_label"),
            )
        )
        .select(TransitMasterSchedule.column_names())
    )

    tm_schedule = logger.log_dataframely_filter_results(*TransitMasterSchedule.filter(stop_crossings))

    return tm_schedule
