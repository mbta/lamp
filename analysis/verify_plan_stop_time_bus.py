import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pyarrow
    import pyarrow.parquet as pq

    return (pl,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(pl):
    df = pl.read_parquet("/tmp/test_bus_recent.parquet")
    tm_event = pl.read_parquet("/tmp/tm_events.parquet")
    gtfs_event = pl.read_parquet("/tmp/gtfs_events.parquet")
    tm_sched = pl.read_parquet("/tmp/tm_schedule.parquet")
    gtfs_sched = pl.read_parquet("/tmp/gtfs_schedule.parquet")
    return df, gtfs_event, gtfs_sched, tm_event


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(pl):
    pl.Config.set_tbl_cols(-1)
    return


@app.cell
def _(cols, df):
    df.select(cols)
    return


@app.cell
def _(cols, df):
    df.drop_nulls("gtfs_travel_to_dt").select(cols).sort("plan_stop_departure_dt")
    return


@app.cell
def _():
    import datetime

    return (datetime,)


@app.cell
def _(datetime, df, pl):
    cols = [
        "plan_start_dt",
        "plan_stop_departure_dt",
        "start_dt",
        "gtfs_travel_to_dt",
        "gtfs_arrival_dt",
        "tm_scheduled_time_dt",
        "tm_actual_arrival_dt",
        "tm_actual_departure_dt",
    ]
    # cols_out = ["plan_start_dt", "plan_stop_departure_dt", "start_dt", "gtfs_travel_to_dt", "gtfs_arrival_dt", "tm_scheduled_time_dt", "tm_actual_arrival_dt", "tm_actual_departure_dt"]
    out_cols = [f"{c}_diff" for c in cols]
    check_all_times_in_timezone = df.drop_nulls(cols).with_columns(
        [
            (pl.col("plan_start_dt").dt.replace_time_zone(None) - pl.col(c).dt.replace_time_zone(None)).alias(
                f"{c}_diff"
            )
            for c in cols
        ]
    ).select(out_cols).sort("gtfs_travel_to_dt_diff", descending=False) < datetime.timedelta(hours=5)
    return check_all_times_in_timezone, cols


@app.cell
def _(check_all_times_in_timezone, pl):
    assert check_all_times_in_timezone.select(pl.all_horizontal(pl.all().all())).item()  # lol
    return


@app.cell
def _(df1):
    df1.sort("plan_stop_departure_dt")
    # plan stop departure - which of these are EST/UTC? Fix it
    return


@app.cell
def _(df1, pl):
    df1.with_columns((pl.col.plan_start_dt - pl.col.plan_stop_departure_dt).alias("timediff")).select("timediff").max()
    return


@app.cell
def _(df1):
    df1["vehicle_label"].unique()
    return


@app.cell
def _(gtfs_sched, pl):
    aa = gtfs_sched.filter(~pl.col("trip_id").str.contains("Blue"))
    aa = aa.filter(~pl.col("trip_id").str.contains("CR"))
    aa = aa.with_columns(pl.col("trip_id").cast(pl.Int32))
    return (aa,)


@app.cell
def _(gtfs_event, pl):
    ol_event = gtfs_event.filter(pl.col("trip_id").str.contains("OL1"))

    return (ol_event,)


@app.cell
def _(ol_event):
    ol_event
    return


@app.cell
def _(gtfs_event, pl):
    bb = gtfs_event.filter(~pl.col("trip_id").str.contains("Blue"))
    bb = bb.filter(~pl.col("trip_id").str.contains("CR"))
    bb = bb.filter(~pl.col("trip_id").str.contains("OL1"))

    bb = bb.with_columns(pl.col("trip_id").cast(pl.Int32))
    return (bb,)


@app.cell
def _(bb):
    bb
    return


@app.cell
def _(aa):
    aa
    return


@app.cell
def _(tm_event):
    tm_event
    return


@app.cell
def _(pl, tm_event):
    tm_event.filter(pl.col("trip_id").str.contains("69703453"))
    return


@app.cell
def _(gtfs_event, pl):
    gtfs_event.filter(pl.col("trip_id").str.contains("69703453"))
    return


@app.cell
def _(df, pl):
    df.filter(pl.col.trip_id == "69703453").sort(by="stop_sequence")
    return


@app.cell
def _(pl):
    df2 = pl.read_parquet(
        "s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_VehiclePositions_LR_60_day.parquet"
    )
    return (df2,)


@app.cell
def _(df2):
    df2["vehicle.trip.start_date"].unique().sort()
    return


@app.cell
def _(df2):
    df2["vehicle.trip.trip_id"].str.contains("LAMP")
    return


@app.cell
def _():
    # df3 = pq.read_table('s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_TripUpdates_LR_60_day.parquet')
    return


@app.cell
def _(df3, pl):
    df4 = pl.from_arrow(df3)
    return (df4,)


@app.cell
def _(df4):
    df4
    return


@app.cell
def _(df4):
    df4.head(1000000)["trip_update.trip.trip_id"].str.contains("LAMP")
    return


@app.cell
def _(df4):
    df4["trip_update.trip.start_date"].unique().sort()
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
    df2["vehicle.trip.start_date"].unique().sort()
    return


if __name__ == "__main__":
    app.run()
