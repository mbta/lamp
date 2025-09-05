from dataclasses import dataclass
import polars as pl

from lamp_py.runtime_utils.remote_files import (
    tm_geo_node_file,
    tm_route_file,
    tm_trip_file,
    tm_vehicle_file,
    tm_time_point_file,
    tm_pattern_geo_node_xref_file,
)


# pylint: disable=R0902
@dataclass(frozen=True)
class TransitMasterSchedule:
    """
    Class holding collection of TM schedule inputs and the resultant tm_schedule for joining with GTFS
    """

    tm_geo_nodes: pl.DataFrame | pl.LazyFrame
    tm_routes: pl.DataFrame | pl.LazyFrame
    tm_vehicles: pl.DataFrame | pl.LazyFrame
    tm_time_points: pl.DataFrame | pl.LazyFrame
    tm_pattern_geo_node_xref: pl.DataFrame | pl.LazyFrame  # to get actual timepoints (non_null)
    tm_pattern_geo_node_xref_full: pl.DataFrame | pl.LazyFrame  # to get all stop sequence (including non-timepoint)
    tm_trip_geo_tp: pl.DataFrame | pl.LazyFrame  # derived
    tm_trip_geo_tp_full: pl.DataFrame | pl.LazyFrame  # derived
    tm_sequences: pl.DataFrame | pl.LazyFrame  # derived
    tm_schedule: pl.DataFrame | pl.LazyFrame


def generate_tm_schedule() -> TransitMasterSchedule:
    """

    non service tiemepoints
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

    # Pattern 1
    # PATTERN_GEO_NODE_SEQ: 1,5,7,9
    # TIMEPOINT_ORDER: 1,2,3,4 (RANK OVER PATTERN GEO SEQ)
    #

    # N patterns (at least 1) per Route - but we don't know Routes yet here
    tm_pattern_geo_node_xref = (
        pl.scan_parquet(tm_pattern_geo_node_xref_file.s3_uri)
        .select("PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID", "GEO_NODE_ID")
        .filter(
            pl.col("TIME_POINT_ID").is_not_null()
            & pl.col("GEO_NODE_ID").is_not_null()
            & pl.col("PATTERN_ID").is_not_null()
            & pl.col("PATTERN_GEO_NODE_SEQ").is_not_null()
        )
    ).with_columns(
        pl.col(["PATTERN_GEO_NODE_SEQ"]).rank(method="dense").over(["PATTERN_ID"]).alias("timepoint_order"),
    )

    # Current
    # ACTUAL: 1, 2, 3, 4, 5 FULL SEQUENCE
    # GTFS:   1, 2,    3, 4  - 3 is the non rev timepoint stop/whatever
    # TM:     1,       4,   (tm_pattern_geo_node_xref)

    # Stop Cross actual events data will only have TM points 1, and 4.

    tm_pattern_geo_node_xref_full = pl.scan_parquet(tm_pattern_geo_node_xref_file.s3_uri).select(
        "PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID", "GEO_NODE_ID"
    )

    # trip id - this is the transit master schedule
    # this is the truth to reference and compare STOP_CROSSING records with to determine timepoint_order
    tm_trip_geo_tp = (
        tm_trips.join(
            tm_pattern_geo_node_xref,
            on="PATTERN_ID",
            how="left",
            coalesce=True,
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID", how="left", coalesce=True)
        .join(tm_time_points, on="TIME_POINT_ID", how="left", coalesce=True)
    )

    tm_trip_geo_tp_full = (
        tm_trips.join(
            tm_pattern_geo_node_xref_full,
            on="PATTERN_ID",
            how="left",
            coalesce=True,
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID", how="left", coalesce=True)
        .join(tm_time_points, on="TIME_POINT_ID", how="left", coalesce=True)
    )
    # TRIP_ID or [TRIP_SERIAL_NUMBER, PATTERN_ID] uniquely identify a TM "Trip".
    # TRIP_SERIAL_NUMBER is the publicly facing number (and gets aliased to "TRIP_ID" below)
    tm_sequences = tm_trip_geo_tp.group_by(["TRIP_ID"]).agg(
        pl.col("PATTERN_GEO_NODE_SEQ").max().alias("tm_planned_sequence_end"),
        pl.col("PATTERN_GEO_NODE_SEQ").min().alias("tm_planned_sequence_start"),
    )

    tm_schedule = tm_trip_geo_tp_full.with_columns(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"), pl.col("GEO_NODE_ABBR").alias("stop_id")
    ).join(
        tm_sequences,
        on="TRIP_ID",
        how="left",
        coalesce=True,
    )

    return TransitMasterSchedule(
        tm_geo_nodes=tm_geo_nodes,
        # tm_trips=tm_trips,
        tm_routes=tm_routes,
        tm_vehicles=tm_vehicles,
        tm_time_points=tm_time_points,
        tm_pattern_geo_node_xref=tm_pattern_geo_node_xref,
        tm_trip_geo_tp=tm_trip_geo_tp,
        tm_trip_geo_tp_full=tm_trip_geo_tp_full,
        tm_pattern_geo_node_xref_full=tm_pattern_geo_node_xref_full,
        tm_sequences=tm_sequences,
        tm_schedule=tm_schedule,
    )
