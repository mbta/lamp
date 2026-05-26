import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return (pl,)


@app.cell
def _(pl):
    vp = pl.read_parquet("prod_vp.parquet")
    tu = pl.read_parquet("prod.parquet")
    return tu, vp


@app.cell
def _(tu):
    tu.columns
    return


@app.cell
def _(vp):
    vp.columns
    return


@app.cell
def _(pl, vp):
    vp_narrow = vp.select("vehicle.trip.trip_id", "vehicle.current_stop_sequence", "vehicle.current_status", "vehicle.position.latitude", "vehicle.position.longitude", "vehicle.vehicle.id","vehicle.timestamp","feed_timestamp").filter(pl.col("vehicle.current_status") == "STOPPED_AT").unique(subset=["vehicle.trip.trip_id", "vehicle.current_stop_sequence"], keep="first").sort("vehicle.trip.trip_id", "vehicle.current_stop_sequence")
    return (vp_narrow,)


app._unparsable_cell(
    r"""
    vp_narrow.
    """,
    name="_"
)


@app.cell
def _(tu, vp_narrow):
    joined = tu.join(vp_narrow, left_on=["trip_update.trip.trip_id", "trip_update.vehicle.id", "trip_update.stop_time_update.stop_sequence"], right_on=["vehicle.trip.trip_id", "vehicle.vehicle.id", "vehicle.current_stop_sequence"], how = "left")
    return (joined,)


@app.cell
def _(joined):
    joined.columns
    return


@app.cell
def _(joined, pl):
    ds = joined.with_columns(true_error=pl.col("trip_update.stop_time_update.arrival.time")- pl.col("vehicle.timestamp"), prediction_ahead=pl.col("feed_timestamp")-pl.col("trip_update.stop_time_update.arrival.time"))
    return (ds,)


@app.cell
def _():
    return


@app.cell
def _(ds, pl):
    dsx = ds.select("trip_update.trip.trip_id","trip_update.stop_time_update.stop_id","trip_update.stop_time_update.stop_sequence", "trip_update.stop_time_update.arrival.time", "vehicle.timestamp", "error", "prediction_ahead", "feed_timestamp").filter(pl.col("trip_update.trip.trip_id") == "75255757").sort("trip_update.stop_time_update.stop_sequence").filter(pl.col("trip_update.stop_time_update.arrival.time").is_not_null()).filter(pl.col("prediction_ahead")>-60*5).filter(pl.col("prediction_ahead")<0)
    return (dsx,)


@app.cell
def _():
    # only need first stopped at for each stop
    return


@app.cell
def _():
    import plotly.express as px
    return (px,)


@app.cell
def _(dsx, px):
    fig = px.scatter(dsx, x="prediction_ahead", y="error", color="trip_update.stop_time_update.stop_sequence")
    fig.show()
    return


if __name__ == "__main__":
    app.run()
