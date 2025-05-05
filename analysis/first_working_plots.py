import marimo

__generated_with = "0.10.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import json
    import altair as alt
    import polars as pl
    import pyarrow
    import pyarrow.parquet as pq
    import pyarrow.dataset as pd
    import pyarrow.compute as pc

    from pyarrow.fs import S3FileSystem
    import os
    from lamp_py.runtime_utils.remote_files import (
        glides_trips_updated,
        glides_operator_signed_in,
        tableau_glides_all_trips_updated,
        tableau_glides_all_operator_signed_in,
    )
    from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_with_details, object_exists
    from lamp_py.ingestion.utils import group_sort_file_list

    return (
        S3FileSystem,
        alt,
        file_list_from_s3,
        file_list_from_s3_with_details,
        glides_operator_signed_in,
        glides_trips_updated,
        group_sort_file_list,
        json,
        mo,
        object_exists,
        os,
        pc,
        pd,
        pl,
        pq,
        pyarrow,
        tableau_glides_all_operator_signed_in,
        tableau_glides_all_trips_updated,
    )


@app.cell
def _(pl, pq):
    filexx = pq.read_table(
        "s3://mbta-ctd-dataplatform-dev-archive/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_HR_30_day.parquet"
    )
    cars = pl.from_arrow(filexx)

    return cars, filexx


@app.cell
def _(cars, pl):
    trip = cars.filter(pl.col("trip_update.trip.trip_id") == "66715265")
    return (trip,)


@app.cell
def _(trip):
    trip
    return


@app.cell
def _(pl, trip):
    from datetime import datetime

    trip2 = trip.with_columns(
        pl.col("feed_timestamp").map_elements(datetime.timestamp).alias("ts"),
        pl.col("trip_update.stop_time_update.stop_sequence").alias("y1"),
    )
    trip2["ts"]
    return datetime, trip2


@app.cell
def _(trip2):
    trip2["ts"]
    return


@app.cell
def _(alt, mo, trip2):
    # strip all completely null columns..
    chart = mo.ui.altair_chart(
        alt.Chart(trip2)
        .mark_point()
        .encode(alt.X("ts:Q").scale(zero=False), alt.Y("y1:Q").scale(zero=False), tooltip=list(trip2.columns))
    )
    return (chart,)


@app.cell
def _(chart):
    chart
    return


if __name__ == "__main__":
    app.run()
