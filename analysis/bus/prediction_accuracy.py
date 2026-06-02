import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import dataframely as dy
    import polars as pl
    return alt, dy, mo, pl


@app.cell
def _(dy):
    class StopEvent(dy.Schema):
        service_date = dy.Date(primary_key = True)
        route_id = dy.String()
        trip_id = dy.String(primary_key = True)
        vehicle_id = dy.String(primary_key = True)
        stop_sequence = dy.Int16(primary_key = True)
        event_type = dy.Enum(categories = ["arrival", "departure"], primary_key = True)
    return (StopEvent,)


@app.cell
def _(StopEvent, dy):
    class Prediction(StopEvent):
        prediction_dt = dy.Datetime(nullable = True)
        predicted_dt = dy.Datetime(primary_key = True)
    return (Prediction,)


@app.cell
def _(StopEvent, dy):
    class Actual(StopEvent):
        actual_dt = dy.Datetime(nullable = True)
    return (Actual,)


@app.cell
def _(Actual, Prediction, dy):
    class Comparison(Actual, Prediction):
        time_variance = dy.Duration(nullable = True)
        time_to_actual = dy.Duration(nullable = True)
    return (Comparison,)


@app.cell
def _(Actual, Comparison, Prediction, StopEvent, dy, pl):
    def make_comparison(predicted: dy.LazyFrame[Prediction], actual: dy.LazyFrame[Actual]) -> dy.LazyFrame[Comparison]:
        """Join predicted and actual events together."""
        lf = (
            actual
            .join(
                predicted,
                on = StopEvent.primary_key(),
                how = "full",
                coalesce = True
            )
            .with_columns(
                time_variance = pl.col("actual_dt") - pl.col("prediction_dt"),
                time_to_actual = pl.col("actual_dt") - pl.col("predicted_dt")
            )
            .select(Comparison.column_names())
        )
        # valid = Comparison.validate(lf, cast = True)
        return lf
    return (make_comparison,)


@app.cell
def _(dy):
    class Accuracy(dy.Schema):
        accuracy = dy.Float(nullable=True, min=0, max=1)
    return (Accuracy,)


@app.cell
def _():
    bounds = {
        "<0m": (0, 30, -2*60, 0),
        "0–3m": (-30, 90, 0, 3*60),
        "3–6m": (-60, 150, 3*60, 6*60),
        "6–10m": (-60, 210, 6*60, 10*60),
        "10–15m": (-90, 270, 10*60, 15*60),
        "15m+": (-120, 330, 15*60, 30*60)
    }
    return (bounds,)


@app.cell
def _(Accuracy, Comparison, bounds, dy, pl):
    def ibi(comparison: dy.LazyFrame[Comparison]) -> dy.LazyFrame[Accuracy]:
        """
        Implement IBI's ETA Accuracy Benchmark.

        More info at https://github.com/TransitApp/ETA-Accuracy-Benchmark
        """
        lf = (
            comparison
            .with_columns(
                time_bucket = pl.col("time_to_actual").dt.total_seconds().cut(
                    breaks = [
                        0,
                        3 * 60,
                        6 * 60,
                        10 * 60,
                        15 * 60
                    ],
                    labels = [
                        "<0m",
                        "0–3m",
                        "3–6m",
                        "6–10m",
                        "10–15m",
                        "15m+",
                    ]
                ),
            )
            # .filter(~pl.col("time_bucket").is_in(["<0m", "15m+"]))
            .with_columns(
                lower_bound=pl.col("time_bucket").cast(pl.String).replace_strict(bounds, default = None, return_dtype = pl.List).list.get(0),
                upper_bound=pl.col("time_bucket").cast(pl.String).replace_strict(bounds, default = None, return_dtype = pl.List).list.get(1)
            )
            .with_columns(
                accuracy = pl.coalesce(
                    pl.col("time_variance").dt.total_seconds().is_between(pl.col("lower_bound"), pl.col("upper_bound")).cast(pl.UInt8),
                    pl.lit(0)
                )
            )
        )

        return lf
    return (ibi,)


@app.cell
def _(pl):
    date = pl.date(2026, 5, 21)
    return (date,)


@app.cell
def _(Actual, pl):
    actual = (
        pl.scan_parquet('~/Downloads/20260521.parquet')
        .filter(
            pl.col("vehicle_label").is_not_null(),
            pl.col("gtfs_stop_sequence").is_not_null(),
            pl.col("gtfs_arrival_dt").is_not_null()
        )
        .with_columns(
            vehicle_id = pl.col("vehicle_label"),
            stop_sequence = pl.col("gtfs_stop_sequence"),
        )
        .with_columns(
            event_type = pl.lit("arrival").cast(pl.Enum(categories = ["arrival", "departure"])),
            actual_dt = pl.col("gtfs_arrival_dt").dt.replace_time_zone("America/New_York")
        )
        .select(Actual.column_names())
    )
    return (actual,)


@app.cell
def _(date, pl):
    predicted = (
        pl.scan_parquet('~/Downloads/2026-05-21T00_00_00.parquet')
        .filter(
            pl.col("trip_update.trip.route_id").str.contains(r"^[1-9]"),
            pl.col("trip_update.vehicle.id").is_not_null(),
            pl.col("trip_update.stop_time_update.stop_sequence").is_not_null(),
            pl.coalesce("trip_update.stop_time_update.arrival.time", "trip_update.stop_time_update.departure.time").is_not_null()
        ) # bus routes are numbered
        .with_columns(
            service_date = pl.col("trip_update.trip.start_date").str.strptime(dtype = pl.Date, format = "%Y%m%d"),
            trip_id = pl.col("trip_update.trip.trip_id"),
            vehicle_id = pl.col("trip_update.vehicle.id").str.replace(pattern = "y|d", value = ""),
            stop_sequence = pl.col("trip_update.stop_time_update.stop_sequence"),
            prediction_dt = pl.from_epoch("trip_update.stop_time_update.arrival.time"),
            predicted_dt = pl.from_epoch("trip_update.timestamp")
        )
        .filter(pl.col("service_date").eq(date))
        # .unique(keep = "first")  # not running this line means every TripUpdate, even those without changed prediction times, are considered; probably biases aggregate accuracy down
        .with_columns(
            pl.selectors.datetime().dt.replace_time_zone("America/New_York"),
            event_type = pl.lit("arrival").cast(pl.Enum(categories = ["arrival", "departure"])),
        )
    )
    return (predicted,)


@app.cell
def _(actual, make_comparison, predicted):
    comparison = make_comparison(predicted, actual)
    return (comparison,)


@app.cell
def _(comparison, pl):
    one_trip = (
        comparison
        .filter(pl.col("trip_id").eq("75609175"))
        .with_columns(pl.selectors.duration().dt.total_seconds())
        .collect()
    )
    return (one_trip,)


@app.cell
def _(one_trip, pl):
    one_trip.filter(pl.col("stop_sequence").eq(3)).with_columns(pl.selectors.datetime().dt.timestamp())
    return


@app.cell
def _(pl):
    pl.int_range(start = 0, end = 1, eager = True).to_frame().with_columns(d = pl.datetime(2026, 5, 21, 8, 50, time_zone="America/New_York").dt.epoch('s'))
    return


@app.cell
def _(alt, mo, one_trip):
    mo.ui.altair_chart(
        alt.Chart(one_trip, title = "Bus Predictions for Trip 75609175 (route 105) on May 21, 2026")
        .mark_trail()
        .encode(
            alt.X("predicted_dt:T").scale(domainMin = 1779367800000).title("Prediction Time"),
            alt.Y("time_variance:Q").title("Prediction - Actual (seconds)"),
            alt.Color("stop_sequence:Q").scale(reverse = True, domainMax = 50).title("Stop Sequence"),
            alt.Size("time_to_actual:Q").scale(reverse = True, type = "symlog").title("Seconds Until Event")
        )
    )
    return


@app.cell
def _(bounds, pl):
    bounds_df = (
        pl.DataFrame(bounds)
        .transpose(
            include_header = True,
            header_name = "time_bucket",
            column_names = ["lower_bound", "upper_bound", "bucket_start", "bucket_end"]
        )
    )
    bounds_df
    return (bounds_df,)


@app.cell
def _(alt, bounds_df):
    chart_bounds = (
        alt.Chart(bounds_df)
        .mark_rect(filled = False, stroke = "green", clip = True)
        .encode(
            alt.X("bucket_start:Q").scale(reverse = True, domain = [0, 900]).title("Time Before Arrival"),
            alt.X2("bucket_end:Q").title(None),
            alt.Y("lower_bound:Q").title("Prediction - Actual Time"),
            alt.Y2("upper_bound:Q").title(None),
        )
    )
    return (chart_bounds,)


@app.cell
def _(comparison, pl):
    binned_comparison = (
        comparison
        .filter(
            pl.col("event_type").eq("arrival"),
            pl.col("time_to_actual").is_between(pl.duration(minutes = -2), pl.duration(minutes = 30)),
        )
        .with_columns(
            pl.selectors.duration().dt.total_seconds().floordiv(30).mul(30)
        )
        .group_by(
            "time_variance",
            "time_to_actual"
        )
        .len()
        .collect()
    )
    return (binned_comparison,)


@app.cell
def _(alt, binned_comparison):
    heatmap = (
        alt.Chart(binned_comparison, title = "Bus Prediction Distribution for May 21, 2026")
        .mark_rect(clip = True)
        .encode(
            alt.X("time_to_actual:Q").bin(step = 30).scale(reverse = True, domain = [0, 900]).axis(labelExpr = "floor(datum.value / 60) + 'm'"),
            alt.Y("time_variance:Q").bin(step = 30).scale(domain = [-5*60, 7*60], clamp = True).axis(labelExpr = "floor(datum.value / 60) + 'm'"),
            alt.Color("len").scale(type = "sqrt").title("Prediction Count")
        )
    )
    return (heatmap,)


@app.cell
def _(chart_bounds, heatmap):
    (heatmap + chart_bounds)
    return


@app.cell
def _(comparison, ibi, pl):
    (
        ibi(comparison)
        .with_columns(
            peak=pl.when(
                pl.col("actual_dt").dt.hour().is_in([7, 8, 9, 16, 17, 18])
            ).then(pl.lit("peak")).otherwise(pl.lit("off-peak"))
        )
        .filter(~pl.col("time_bucket").is_in(["<0m", "15m+"]))
        .group_by(
            "route_id",
            # "peak",
            # "time_bucket",
            # "event_type"
        )
        .agg(accuracy=pl.col("accuracy").mean(), count=pl.col("accuracy").count(), inaccurate_count = (pl.lit(1) - pl.col("accuracy")).sum())
        .collect()
    )
    return


if __name__ == "__main__":
    app.run()
