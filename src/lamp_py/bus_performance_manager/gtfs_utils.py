from datetime import date
from typing import List

import polars as pl

from lamp_py.runtime_utils.remote_files import get_gtfs_parquet_file


def bus_routes_for_service_date(service_date: date) -> List[str]:
    """get a list of bus route ids for a given service date"""
    routes_file = get_gtfs_parquet_file(
        year=service_date.year, filename="routes.parquet"
    ).get_s3_path()

    # generate an integer date for the service date
    target_date = int(service_date.strftime("%Y%m%d"))

    bus_routes = (
        pl.scan_parquet(routes_file)
        .select(["route_id", "route_type", "gtfs_active_date", "gtfs_end_date"])
        .filter(
            (pl.col("gtfs_active_date") <= target_date)
            & (pl.col("gtfs_end_date") >= target_date)
            & (pl.col("route_type") == 3)
        )
        .collect()
    )

    return bus_routes["route_id"].unique().to_list()
