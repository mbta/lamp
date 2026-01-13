import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    return (pl,)


@app.cell
def _():
    import os

    return (os,)


@app.cell
def _():
    from datetime import date

    return (date,)


@app.cell
def _():
    from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

    return (FilteredHyperJob,)


@app.cell
def _():
    from lamp_py.runtime_utils.remote_files import (
        LAMP,
        S3_ARCHIVE,
        S3Location,
        bus_events,
    )

    return LAMP, S3Location, S3_ARCHIVE, bus_events


@app.cell
def _():
    # August 24, 2025
    # December 13, 2025
    # 16 weeks
    return


@app.cell
def _():
    # first - regenerate all of these - do they have the current schema?
    return


@app.cell
def _(LAMP, S3Location, S3_ARCHIVE, os):
    tableau_bus_fall_rating = S3Location(
        bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "bus_rating_datasets", "year=2025", "Fall.parquet"), version="1.0"
    )
    return (tableau_bus_fall_rating,)


@app.cell
def _(tableau_bus_fall_rating):
    tableau_bus_fall_rating
    return


@app.cell
def _():
    from lamp_py.tableau.jobs.bus_performance import bus_schema

    return (bus_schema,)


@app.cell
def _():
    from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions

    return (apply_bus_analysis_conversions,)


@app.cell
def _(bus_events):
    bus_events
    return


@app.cell
def _():
    GTFS_RT_TABLEAU_PROJECT = "GTFS-RT"

    return (GTFS_RT_TABLEAU_PROJECT,)


@app.cell
def _(
    FilteredHyperJob,
    GTFS_RT_TABLEAU_PROJECT,
    apply_bus_analysis_conversions,
    bus_events,
    bus_schema,
    date,
    tableau_bus_fall_rating,
):
    HyperBus = FilteredHyperJob(
        remote_input_location=bus_events,
        remote_output_location=tableau_bus_fall_rating,
        start_date=date(2025, 8, 24),
        # start_date=date(2025, 12,12),
        end_date=date(2025, 12, 13),
        processed_schema=bus_schema,
        dataframe_filter=apply_bus_analysis_conversions,
        parquet_filter=None,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
        partition_template="{yy}{mm:02d}{dd:02d}.parquet",
    )
    return (HyperBus,)


@app.cell
def _(HyperBus):
    HyperBus.run_parquet(None)
    return


@app.cell
def _(pl):
    df = pl.read_parquet("s3://mbta-ctd-dataplatform-dev-archive/lamp/bus_rating_datasets/year=2025/Fall.parquet")
    return (df,)


@app.cell
def _(df):
    df["is_full_trip"]
    return


@app.cell
def _(pl):
    df824 = pl.read_parquet("s3://mbta-ctd-dataplatform-dev-archive/lamp/bus_vehicle_events/20250824.parquet")
    return (df824,)


@app.cell
def _(df824):
    df824["is_full_trip"]
    return


if __name__ == "__main__":
    app.run()
