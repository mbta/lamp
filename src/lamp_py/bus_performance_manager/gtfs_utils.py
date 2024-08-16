from datetime import date
from typing import List

import polars as pl

from lamp_py.runtime_utils.remote_files import compressed_gtfs


def bus_routes_for_service_date(service_date: date) -> List[str]:
    """get a list of bus route ids for a given service date"""
    routes_file = compressed_gtfs.parquet_path(
        year=service_date.year, file="routes"
    ).s3_uri

    # generate an integer date for the service date
    target_date = int(service_date.strftime("%Y%m%d"))

    bus_routes = (
        pl.scan_parquet(routes_file)
        .filter(
            (pl.col("gtfs_active_date") <= target_date)
            & (pl.col("gtfs_end_date") >= target_date)
            & (pl.col("route_type") == 3)
        )
        .select("route_id")
        .unique()
        .collect()
    )

    return bus_routes.get_column("route_id").to_list()
