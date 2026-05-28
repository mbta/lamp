import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import dataframely as dy
    import polars as pl

    return dy, pl


@app.cell
def _(dy):
    class StopEvent(dy.Schema):
        service_date = dy.Date(primary_key=True)
        route_id = dy.String()
        trip_id = dy.String(primary_key=True)
        vehicle_id = dy.String(primary_key=True)
        stop_sequence = dy.Int16(primary_key=True)
        event_type = dy.Enum(categories=["arrival", "departure"], primary_key=True)

    return (StopEvent,)


@app.cell
def _(StopEvent, dy):
    class Prediction(StopEvent):
        prediction_dt = dy.Datetime(nullable=True)
        predicted_dt = dy.Datetime(primary_key=True)

    return (Prediction,)


@app.cell
def _(StopEvent, dy):
    class Actual(StopEvent):
        actual_dt = dy.Datetime(nullable=True)

    return (Actual,)


@app.cell
def _(Actual, Prediction, dy):
    class Comparison(Actual, Prediction):
        time_variance = dy.Duration(nullable=True)
        time_to_actual = dy.Duration(nullable=True)

    return (Comparison,)


@app.cell
def _(Actual, Comparison, Prediction, StopEvent, dy, pl):
    def make_comparison(predicted: dy.LazyFrame[Prediction], actual: dy.LazyFrame[Actual]) -> dy.LazyFrame[Comparison]:
        """Join predicted and actual events together."""
        lf = (
            actual.join(predicted, on=StopEvent.primary_key(), how="full", coalesce=True)
            .with_columns(
                time_variance=pl.col("actual_dt") - pl.col("prediction_dt"),
                time_to_actual=pl.col("actual_dt") - pl.col("predicted_dt"),
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
def _(Accuracy, Comparison, dy, pl):
    def ibi(comparison: dy.LazyFrame[Comparison]) -> dy.LazyFrame[Accuracy]:
        """
        Implement IBI's ETA Accuracy Benchmark.

        More info at https://github.com/TransitApp/ETA-Accuracy-Benchmark
        """
        lower_bound = {"<0m": -10, "0–3m": -30, "3–6m": -60, "6–10m": -60, "10–15m": -90, "15m+": -120}

        upper_bound = {"<0m": 10, "0–3m": 90, "3–6m": 150, "6–10m": 210, "10–15m": 270, "15m+": 330}

        lf = (
            comparison.with_columns(
                time_bucket=pl.col("time_to_actual")
                .dt.total_seconds()
                .cut(
                    breaks=[0, 3 * 60, 6 * 60, 10 * 60, 15 * 60],
                    labels=[
                        "<0m",
                        "0–3m",
                        "3–6m",
                        "6–10m",
                        "10–15m",
                        "15m+",
                    ],
                ),
            )
            # .filter(~pl.col("time_bucket").is_in(["<0m", "15m+"]))
            .with_columns(
                lower_bound=pl.col("time_bucket")
                .cast(pl.String)
                .replace_strict(lower_bound, default=None, return_dtype=pl.Int16),
                upper_bound=pl.col("time_bucket")
                .cast(pl.String)
                .replace_strict(upper_bound, default=None, return_dtype=pl.Int16),
            )
            .with_columns(
                accuracy=pl.coalesce(
                    pl.col("time_variance")
                    .dt.total_seconds()
                    .is_between(pl.col("lower_bound"), pl.col("upper_bound"))
                    .cast(pl.UInt8),
                    pl.lit(0),
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
        pl.scan_parquet("~/Downloads/20260521.parquet")
        .filter(
            pl.col("vehicle_label").is_not_null(),
            pl.col("gtfs_stop_sequence").is_not_null(),
            pl.coalesce("gtfs_arrival_dt", "gtfs_departure_dt").is_not_null(),
        )
        .with_columns(
            vehicle_id=pl.col("vehicle_label"),
            stop_sequence=pl.col("gtfs_stop_sequence"),
        )
        .unpivot(
            index=[
                "service_date",
                "trip_id",
                "vehicle_id",
                "stop_sequence",
                "route_id",
            ],
            variable_name="event_type",
            value_name="actual_dt",
            on=["gtfs_arrival_dt", "gtfs_departure_dt"],
        )
        .with_columns(
            event_type=pl.when(pl.col("event_type").eq("gtfs_arrival_dt"))
            .then(pl.lit("arrival"))
            .otherwise(pl.lit("departure"))
            .cast(pl.Enum(categories=["arrival", "departure"])),
            actual_dt=pl.col("actual_dt").dt.replace_time_zone("America/New_York"),
        )
        .select(Actual.column_names())
    )
    return (actual,)


@app.cell
def _(date, pl):
    predicted = (
        pl.scan_parquet("~/Downloads/2026-05-21T00_00_00.parquet")
        .filter(
            pl.col("trip_update.trip.route_id").str.contains(r"^[1-9]"),
            pl.col("trip_update.vehicle.id").is_not_null(),
            pl.col("trip_update.stop_time_update.stop_sequence").is_not_null(),
            pl.coalesce(
                "trip_update.stop_time_update.arrival.time", "trip_update.stop_time_update.departure.time"
            ).is_not_null(),
        )  # bus routes are numbered
        .with_columns(
            service_date=pl.col("trip_update.trip.start_date").str.strptime(dtype=pl.Date, format="%Y%m%d"),
            trip_id=pl.col("trip_update.trip.trip_id"),
            vehicle_id=pl.col("trip_update.vehicle.id").str.replace(pattern="y|d", value=""),
            stop_sequence=pl.col("trip_update.stop_time_update.stop_sequence"),
            arrival=pl.from_epoch("trip_update.stop_time_update.arrival.time"),
            departure=pl.from_epoch("trip_update.stop_time_update.departure.time"),
            predicted_dt=pl.from_epoch("trip_update.timestamp"),
        )
        .filter(pl.col("service_date").eq(date))
        # .unique(keep = "first")  # not running this line means every TripUpdate, even those without changed prediction times, are considered; probably biases aggregate accuracy down
        .unpivot(
            index=[
                "service_date",
                "trip_id",
                "vehicle_id",
                "stop_sequence",
                "predicted_dt",
            ],
            on=["arrival", "departure"],
            value_name="prediction_dt",
            variable_name="event_type",
        )
        .with_columns(
            pl.selectors.datetime().dt.replace_time_zone("America/New_York"),
            event_type=pl.col("event_type").cast(pl.Enum(categories=["arrival", "departure"])),
        )
    )
    return (predicted,)


@app.cell
def _(actual, make_comparison, predicted):
    comparison = make_comparison(predicted, actual)
    return (comparison,)


@app.cell
def _(comparison, ibi, pl):
    (
        ibi(comparison)
        .with_columns(
            peak=pl.when(pl.col("actual_dt").dt.hour().is_in([7, 8, 9, 16, 17, 18]))
            .then(pl.lit("peak"))
            .otherwise(pl.lit("off-peak"))
        )
        .group_by("route_id", "peak", "time_bucket")
        .agg(accuracy=pl.col("accuracy").mean(), count=pl.col("accuracy").count())
        .collect()
    )
    return


if __name__ == "__main__":
    app.run()
