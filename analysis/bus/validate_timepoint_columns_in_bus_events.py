import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pprint import pprint
    from analysis import prism
    import pyarrow
    import pyarrow.parquet as pq
    import pyarrow.dataset as pd
    import pyarrow.compute as pc

    # import plotly.express as px

    from lamp_py.aws.s3 import (
        file_list_from_s3,
        file_list_from_s3_with_details,
        object_exists,
        file_list_from_s3_date_range,
    )

    from pyarrow.fs import S3FileSystem
    import os

    return pl, prism


@app.cell
def _(pl):
    df = pl.read_parquet(
        "/Users/hhuang/Library/CloudStorage/OneDrive-MBTA/LAMP/Work/bus_data_opsanalytics/0_CombinedSchedule/20250904/20250814_bus_recent.parquet"
    )
    return (df,)


@app.cell
def _(df, pl):
    df.filter(pl.col("trip_id") == "69703044").sort("stop_sequence")
    return


@app.cell
def _(df, pl):
    assert df.with_columns(
        pl.when(
            (pl.col("timepoint_order").is_not_null() & pl.col("timepoint_id").is_not_null())
            | (pl.col("timepoint_order").is_null() & pl.col("timepoint_id").is_null())
        ).then(pl.lit(True).alias("verify_timepoint_order"))
    )["verify_timepoint_order"].all()
    return


@app.cell
def _(df, prism):
    prism.assert_timepoint_order_timepoint_id_correspond(df)
    return


@app.cell
def _(df, pl):
    assert (
        df.with_columns(pl.col("timepoint_order").sort().diff().over("trip_id").alias("timepoint_order_diff"))[
            "timepoint_order_diff"
        ]
        .drop_nulls()
        .eq(1)
        .all()
    )
    return


if __name__ == "__main__":
    app.run()
