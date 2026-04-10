import marimo

__generated_with = "0.16.5"
app = marimo.App()


@app.cell
def _(mo):
    mo.md(
        r"""
    # Flashback Usability Indicators

    Our users need to know what assumptions they should make about our data to use it effectively.
    """
    )
    return


@app.cell
def _():
    from datetime import datetime, timedelta
    import marimo as mo
    import polars as pl

    from lamp_py.flashback.events import StopEvents
    from lamp_py.aws.s3 import file_list_from_s3_with_details

    return StopEvents, datetime, mo, pl, timedelta


@app.cell
def _(mo):
    refresher = mo.ui.refresh(options=[5, 10, 60], default_interval=10)
    return


@app.cell
def _(mo):
    env = mo.ui.dropdown(options=["staging", "dev"], value="staging")
    return (env,)


@app.cell
def _(env):
    bucket_name = f"mbta-ctd-dataplatform-{env.value}-archive"
    return (bucket_name,)


@app.cell
def _():
    file_prefix = "lamp/stop_events/stop_events_v0.json.gz"
    return (file_prefix,)


@app.cell
def _(datetime, timedelta):
    flashback_horizon = (datetime.now() - timedelta(hours=2)).timestamp()
    return (flashback_horizon,)


@app.cell
def _(StopEvents, bucket_name, file_prefix, pl):
    stop_events = pl.scan_ndjson(f"s3://{bucket_name}/{file_prefix}", schema=StopEvents.to_polars_schema())
    return (stop_events,)


@app.cell
def _(pl, stop_events):
    unique_dates = (
        stop_events.select(pl.from_epoch("latest_stopped_timestamp").dt.date().alias("date"))
        .unique()
        .collect()
        .to_series()
        .to_list()
    )
    return (unique_dates,)


@app.cell
def _(flashback_horizon, pl, unique_dates):
    vp = (
        pl.scan_parquet(
            [
                d.strftime(
                    "s3://mbta-ctd-dataplatform-springboard/lamp/RT_VEHICLE_POSITIONS/year=%Y/month=%-m/day=%-d/%Y-%m-%dT00:00:00.parquet"
                )
                for d in unique_dates
            ]
        )
        .filter(pl.col("vehicle.current_status").eq("STOPPED_AT"), pl.col("vehicle.timestamp").gt(flashback_horizon))
        .group_by(
            [
                pl.col("vehicle.trip.route_id").alias("route_id"),
                pl.col("vehicle.trip.trip_id").alias("trip_id"),
                pl.col("vehicle.vehicle.id").alias("vehicle_id"),
                pl.col("vehicle.current_stop_sequence").alias("stop_sequence"),
                pl.col("vehicle.trip.start_date").alias("start_date"),
            ]
        )
        .agg(
            pl.from_epoch("vehicle.timestamp")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
            .min()
            .alias("arrived"),
            pl.from_epoch("vehicle.timestamp")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
            .max()
            .alias("departed"),
        )
    )
    return (vp,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Coverage

    If an event happens, will I see it?

    $$
    coverage = \frac
    {Observed Events}
    {Total Events}
    = 1 - \frac
    {MissingEvents}
    {TotalEvents}
    $$

    We can approximate total events as the events that appear in our `VehiclePositions` archive with date, trip, route, and vehicle information. We expect arrivals for every stop after the first stop on a trip and departures for every stop before the last stop on a trip.
    """
    )
    return


@app.cell
def _(StopEvents, pl, vp):
    plausible_events = vp.unpivot(on=["arrived", "departed"], index=StopEvents.primary_key()).filter(
        pl.all_horizontal(
            pl.selectors.by_name(StopEvents.primary_key()).is_not_null()
        ),  # remove events without complete identifiers
        pl.col("stop_sequence").gt(1).or_(pl.col("variable") != "arrived"),  # remove arrivals at startpoints
    )
    return (plausible_events,)


@app.cell
def _(StopEvents, plausible_events, stop_events):
    missing_events = plausible_events.join(
        stop_events.unpivot(on=["arrived", "departed"], index=StopEvents.primary_key()),
        on=[*StopEvents.primary_key(), "variable"],
        how="anti",
    ).collect()
    return (missing_events,)


@app.cell
def _(missing_events, plausible_events):
    missing = missing_events.height / plausible_events.collect().height
    return (missing,)


@app.cell(hide_code=True)
def _(missing, mo):
    mo.stat(
        value=f"{1 - missing:.1%}",
        caption=f"of events within the last 2 hours",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Performance

    How quickly can we process an event after it appears in `VehiclePositions`?

    $$
    performance = timestamp_{Flashback} - timestamp_{VehiclePositions}
    $$

    [Splunk captures this data as `duration` on an ongoing basis](https://mbta.splunkcloud.com/en-US/app/search/search?q=search%20index%3Dlamp-*%20process_name%3Dflashback%20status%3Dcomplete%20%7C%20bin%20duration%20span%3D3%20as%20duration%20%7C%20stats%20count(_raw)%20BY%20index%2C%20duration%20%7C%20sort%20index%2C%20duration&display.page.search.mode=verbose&dispatch.sample_ratio=1&workload_pool=&earliest=-24h%40h&latest=now&display.page.search.tab=statistics&display.general.type=statistics).
    """
    )
    return


@app.cell
def _(mo):
    mo.stat("99.96%", caption="of updates are processed within 4 seconds")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Accuracy

    When I see an event, does it have the correct arrival and departure times?

    $$
    accuracy = |timestamp_{event} - timestamp_{observation}|
    $$

    To define correct, we'll use our less performant `VehiclePositions` dataset again. Since these 2 datasets poll the `VehiclePositions` data at different frequencies, they may observe different timestamps from vehicles. As such, we should tolerate a small difference in those timestamps.
    """
    )
    return


@app.cell
def _(StopEvents, pl, plausible_events, stop_events):
    event_accuracy = (
        plausible_events.join(
            stop_events.unpivot(on=["arrived", "departed"], index=StopEvents.primary_key()).with_columns(
                pl.from_epoch("value").dt.replace_time_zone("UTC").dt.convert_time_zone("America/New_York")
            ),
            on=[*StopEvents.primary_key(), "variable"],
            how="left",
            suffix="_right",
        )
        .select(
            accuracy=pl.col("value").sub(pl.col("value_right")).abs().quantile(0.99).dt.total_seconds(fractional=False)
        )
        .collect()
    )
    return (event_accuracy,)


@app.cell
def _(event_accuracy, mo):
    mo.stat(
        f"{event_accuracy.row()[0]} seconds",
        caption="99th percentile difference between observed and actual event timestamps",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Latency

    After an event occurs, how long do I need to wait before I see it?

    $$
    latency = timestamp_{publication} - timestamp_{event}
    $$

    This metric is meaningful—but is mostly outside our control: after an event occurs, its data must travel through source and TID systems before Flashback processes it.
    """
    )
    return


@app.cell
def _(pl, stop_events):
    event_lag = (
        stop_events.with_columns(
            (pl.from_epoch("timestamp") - pl.from_epoch(pl.max_horizontal("arrived", "departed"))).alias("event_lag"),
            pl.when(pl.col("departed").is_not_null())
            .then(pl.lit("departed"))
            .otherwise(pl.lit("arrived"))
            .alias("most_recent_event"),
        )
        .select(pl.col("most_recent_event"), pl.col("event_lag"))
        .collect()
    )
    return (event_lag,)


@app.cell
def _(event_lag, pl):
    arrival_lag = (
        event_lag.filter(pl.col("event_lag") <= pl.duration(seconds=1), pl.col("most_recent_event") == "arrived").height
        / event_lag.filter(pl.col("most_recent_event") == "arrived").height
    )
    return (arrival_lag,)


@app.cell
def _(arrival_lag, mo):
    mo.stat(value=f"{arrival_lag:.1%}", caption="of observed arrivals published within 15 seconds of arriving")
    return


@app.cell
def _(event_lag, pl):
    departure_lag = (
        event_lag.filter(
            pl.col("event_lag") <= pl.duration(seconds=15), pl.col("most_recent_event") == "departed"
        ).height
        / event_lag.filter(pl.col("most_recent_event") == "departed").height
    )
    return (departure_lag,)


@app.cell
def _(departure_lag, mo):
    mo.stat(value=f"{departure_lag:.1%}", caption="of observed departures published within 15 seconds of departing")
    return


if __name__ == "__main__":
    app.run()
