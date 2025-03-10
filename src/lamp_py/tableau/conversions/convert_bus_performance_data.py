import polars as pl
from pyarrow import Table


def apply_bus_analysis_conversions(polars_df: pl.DataFrame) -> Table:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    # Convert datetime to Eastern Time
    polars_df = polars_df.with_columns(
        pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
        pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
        pl.col("gtfs_travel_to_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
    )

    # Convert seconds columns to be aligned with Eastern Time
    polars_df = polars_df.with_columns(
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

    polars_df = polars_df.with_columns(pl.col("service_date").str.strptime(pl.Date, "%Y%m%d", strict=False))

    return polars_df.to_arrow()
