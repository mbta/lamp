import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pyarrow
    import pyarrow.parquet as pq

    return pl, pq


@app.cell
def _(pl):
    df = pl.read_parquet('/tmp/plan_stop_250814_bus_recent.parquet')
    return (df,)


@app.cell
def _(df, pl):
    df.filter(pl.col.trip_id == "69703453").sort(by="stop_sequence")
    return


@app.cell
def _(pl):
    df2 = pl.read_parquet('s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_VehiclePositions_LR_60_day.parquet')
    return (df2,)


@app.cell
def _(pq):
    df3 = pq.read_table('s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_TripUpdates_LR_60_day.parquet')
    return (df3,)


@app.cell
def _(df3, pl):
    df4 = pl.from_arrow(df3)
    return (df4,)


@app.cell
def _(df4):
    df4['trip_update.trip.start_date'].unique()
    return


@app.cell
def _(df3):
    df3.collect()
    return


@app.cell
def _(df3):
    df3
    return


@app.cell
def _(df2):
    df2['vehicle.trip.start_date'].unique().sort()
    return


if __name__ == "__main__":
    app.run()
