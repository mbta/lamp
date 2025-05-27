from datetime import date
from typing import List
import polars as pl

from lamp_py.aws.s3 import object_exists
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import compressed_gtfs


def gtfs_from_parquet(file: str, service_date: date) -> pl.DataFrame:
    """
    Get GTFS data from specified file and service date

    This will read from s3_uri of file

    :param file: gtfs file to acces (i.e. "feed_info")
    :param service_date: service date of requested GTFS data

    :return dataframe:
        data columns of parquet file for service_date
    """
    logger = ProcessLogger("gtfs_from_parquet", file=file, service_date=service_date)
    logger.log_start()

    gtfs_year = service_date.year
    service_date_int = int(service_date.strftime("%Y%m%d"))

    gtfs_file = compressed_gtfs.parquet_path(gtfs_year, file).s3_uri

    if not object_exists(gtfs_file):
        gtfs_file = compressed_gtfs.parquet_path(gtfs_year - 1, file).s3_uri
        if not object_exists(gtfs_file):
            exception = FileNotFoundError(f"No GTFS archive files available for {service_date}")
            logger.log_failure(exception)
            raise exception

    logger.add_metadata(gtfs_file=gtfs_file)

    gtfs_df = (
        pl.read_parquet(gtfs_file)
        .filter(
            (pl.col("gtfs_active_date") <= service_date_int),
            (pl.col("gtfs_end_date") >= service_date_int),
        )
        .drop(["gtfs_active_date", "gtfs_end_date"])
    )
    logger.add_metadata(gtfs_row_count=gtfs_df.shape[0])
    logger.log_complete()
    return gtfs_df


def bus_route_ids_for_service_date(service_date: date) -> List[str]:
    """get a list of bus route ids for a given service date"""
    bus_routes = (
        gtfs_from_parquet("routes", service_date).filter((pl.col("route_type") == 3)).get_column("route_id").unique()
    )

    return bus_routes.to_list()

def routes_for_service_date(service_date: date) -> pl.DataFrame:
    """get a list of all routes for a given service date"""
    routes = (
        gtfs_from_parquet("routes", service_date)
    )

    return routes