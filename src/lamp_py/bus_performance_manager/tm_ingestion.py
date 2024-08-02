from typing import List
from datetime import date

import pytz
import polars as pl

from lamp_py.runtime_utils.remote_files import RemoteFileLocations

BOSTON_TZ = pytz.timezone("EST5EDT")
UTC_TZ = pytz.utc


def create_dt_from_sam(
    service_date_col: pl.Expr, sam_time_col: pl.Expr
) -> pl.Expr:
    """
    add a seconds after midnight to a service date to create a datetime object.
    seconds after midnight is in boston local time, convert it to utc.
    """
    return (
        service_date_col.cast(pl.Datetime) + pl.duration(seconds=sam_time_col)
    ).map_elements(
        lambda x: BOSTON_TZ.localize(x).astimezone(UTC_TZ),
        return_dtype=pl.Datetime,
    )


def generate_tm_events(tm_files: List[str]) -> pl.DataFrame:
    """
    build out events from transit master stop crossing data
    """
    # the geo node id is the transit master key and the geo node abbr is the
    # gtfs stop id
    tm_geo_nodes = pl.scan_parquet(
        RemoteFileLocations.tm_geo_node_file.get_s3_path()
    ).select(["GEO_NODE_ID", "GEO_NODE_ABBR"])

    # the route id is the transit master key and the route abbr is the gtfs
    # route id.
    # NOTE: some of these route ids have leading zeros
    tm_routes = pl.scan_parquet(
        RemoteFileLocations.tm_route_file.get_s3_path()
    ).select(["ROUTE_ID", "ROUTE_ABBR"])

    # the trip id is the transit master key and the trip serial number is the
    # gtfs trip id.
    tm_trips = pl.scan_parquet(
        RemoteFileLocations.tm_trip_file.get_s3_path()
    ).select(["TRIP_ID", "TRIP_SERIAL_NUMBER"])

    # the vehicle id is the transit master key and the property tag is the
    # vehicle label
    tm_vehicles = pl.scan_parquet(
        RemoteFileLocations.tm_vehicle_file.get_s3_path()
    ).select(["VEHICLE_ID", "PROPERTY_TAG"])

    # pull stop crossing information for a given service date and join it with
    # other dataframes using the transit master keys.
    #
    # convert the calendar id to a date object
    # remove leading zeros from route ids where they exist
    # convert arrival and departure times to utc datetimes
    # cast everything else as a string
    tm_stop_crossings = (
        pl.scan_parquet(tm_files)
        .filter(
            pl.col("ACT_ARRIVAL_TIME").is_not_null()
            | pl.col("ACT_DEPARTURE_TIME").is_not_null()
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID")
        .join(tm_routes, on="ROUTE_ID")
        .join(tm_trips, on="TRIP_ID", how="left", coalesce=True)
        .join(tm_vehicles, on="VEHICLE_ID")
        .select(
            pl.col("CALENDAR_ID")
            .cast(pl.Utf8)
            .str.slice(1)
            .str.strptime(pl.Date, format="%Y%m%d")
            .alias("service_date"),
            pl.col("ACT_ARRIVAL_TIME").alias("arrival_sam"),
            pl.col("ACT_DEPARTURE_TIME").alias("departure_sam"),
            pl.col("PROPERTY_TAG").cast(pl.String).alias("vehicle_label"),
            pl.col("ROUTE_ABBR")
            .cast(pl.String)
            .str.strip_chars_start("0")
            .alias("route_id"),
            pl.col("GEO_NODE_ID").cast(pl.String).alias("geo_node_id"),
            pl.col("GEO_NODE_ABBR").cast(pl.String).alias("stop_id"),
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
        )
        .with_columns(
            create_dt_from_sam(
                pl.col("service_date"), pl.col("arrival_sam")
            ).alias("arrival_tm"),
            create_dt_from_sam(
                pl.col("service_date"), pl.col("departure_sam")
            ).alias("departure_tm"),
        )
        .collect()
    )

    return tm_stop_crossings
