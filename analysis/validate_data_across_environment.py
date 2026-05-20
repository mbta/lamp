import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from analysis.rowgroup_analysis import get_rowgroup_statistics
    from analysis.prism import ge
    return get_rowgroup_statistics, pl


@app.cell
def _(pl):
    df = pl.scan_parquet('s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet', missing_columns="insert")
    return (df,)


@app.cell
def _():
    import pyarrow.dataset as ds
    dset = ds.dataset('s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet', format="parquet")

    return (dset,)


@app.cell
def _(dset, pl):
    pl.scan_pyarrow_dataset(dset).filter(pl.col('trip_update.trip.route_id') == "1").collect()
    return


@app.cell
def _(df):
    routes = df.select('trip_update.trip.route_id').unique().collect()
    return (routes,)


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(routes):
    routes.sort(by='trip_update.trip.route_id')
    return


@app.cell
def _(get_rowgroup_statistics):
    get_rowgroup_statistics("s3://mbta-ctd-dataplatform-staging-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=1/2026-05-01T00:00:00.parquet")
    return


@app.cell
def _(df, pl):
    df1 = df.filter(pl.col('trip_update.trip.route_id') == "1").collect()
    return


if __name__ == "__main__":
    app.run()
