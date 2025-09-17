import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pyarrow
    import pyarrow.dataset as ds
    from pyarrow.fs import S3FileSystem
    import plotly.express as px
    return S3FileSystem, ds, mo, pl, px


@app.cell
def _():
    vehicle_id = "G-10204"
    trip_id = "71193618"
    return trip_id, vehicle_id


@app.cell
def _(px):

    def plot_lla(vp):
        fig3 = px.scatter_map(vp, lat="vehicle.position.latitude", lon="vehicle.position.longitude", zoom=8, height=300, hover_data="vehicle.timestamp")
        fig3.update_layout(mapbox_style="open-street-map")
        fig3.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        config2 = {'scrollZoom': True}
        return fig3.show(config2)
    return (plot_lla,)


@app.cell
def _(ds_helper, pl, vehicle_id):
    vp_raw = ds_helper(["s3://mbta-ctd-dataplatform-springboard/lamp/RT_VEHICLE_POSITIONS/year=2025/month=9/day=17/2025-09-17T00:00:00.parquet"], ( pl.col('id') == vehicle_id))
    return (vp_raw,)


@app.cell
def _(mo, vp_raw):
    mo.ui.dataframe(vp_raw)
    return


@app.cell
def _(pl, plot_lla, trip_id, vp_raw):
    plot_lla(vp_raw.filter(pl.col('vehicle.trip.trip_id') == trip_id))
    return


@app.cell
def _(S3FileSystem, ds, pl):
    def ds_helper(s3_uris, filter):
        ds_paths = [s.replace("s3://", "") for s in s3_uris]
        dset = ds.dataset(ds_paths, format="parquet",  filesystem=S3FileSystem())
        if filter is not None:
            aa = pl.scan_pyarrow_dataset(dset).filter(filter).collect()
        else:
            aa = pl.scan_pyarrow_dataset(dset).collect()
        return aa
    return (ds_helper,)


if __name__ == "__main__":
    app.run()
