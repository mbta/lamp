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
        service_date = dy.Date(primary_key = True)
        trip_id = dy.String(primary_key = True)
        vehicle_id = dy.String(primary_key = True)
        stop_sequence = dy.String(primary_key = True)
        event_type = dy.Enum(categories = ["arrival", "departure"], primary_key = True)
    return (StopEvent,)


@app.cell
def _(StopEvent, dy):
    class Prediction(StopEvent):
        prediction_dt = dy.Datetime(nullable = True)
        predicted_dt = dy.Datetime(nullable = True)
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
def _(Actual, Comparison, Prediction, StopEvent, dy):
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
        )
        valid = Comparison.validate(lf)
        return valid
    return


@app.cell
def _(dy):
    class Accuracy(dy.Schema):
        accuracy = dy.Float(nullable=True, min=0, max=0)
    return (Accuracy,)


@app.cell
def _(Accuracy, Comparison, dy, pl):
    def ibi(comparison: dy.LazyFrame[Comparison]) -> dy.LazyFrame[Accuracy]:
        """
        Implement IBI's ETA Accuracy Benchmark.

        More info at https://github.com/TransitApp/ETA-Accuracy-Benchmark
        """
        accuracy_thresholds = {
            "0–3m": (-30, 90),
            "3–6m": (-60, 150),
            "6–10m": (-60, 210),
            "10–15m": (-90, 270),
        }

        lf = (
            comparison
            .with_columns(
                time_bucket = pl.col("time_to_actual").cut(
                    breaks = [
                        pl.duration(seconds = 0),
                        pl.duration(minutes = 3),
                        pl.duration(minutes = 6),
                        pl.duration(minutes = 10),
                        pl.duration(minutes = 15)
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
            .filter(~pl.col("time_bucket").is_in(["0–3m", "15m+"]))
            .with_columns(
                accuracy=pl.col("time_variance").is_between(
                    pl.col("time_bucket").replace(accuracy_thresholds)
                )
            )
        )
    return


@app.cell
def _(pl):
    date = pl.date(2026, 5, 21)
    return


@app.cell
def _(Actual, pl):
    actual = (
        pl.scan_parquet('s3://mbta-performance/lamp/bus_vehicle_events/20260521.parquet')
        .with_columns(
            vehicle_id = pl.col("vehicle_label"),
            stop_sequence = pl.col("gtfs_stop_sequence"),
        )
        .unpivot(
            index = [
                "service_date",
                "trip_id",
                "vehicle_id",
                "stop_sequence",
            ],
            variable_name = "event_type",
            value_name = "actual_dt",
            on = ["gtfs_arrival_dt", "gtfs_departure_dt"]
        )
        .with_columns(
            event_type = pl.when(pl.col("event_type").eq("gtfs_arrival_dt"))
                .then(pl.lit("arrival"))
                .otherwise(pl.lit("departure"))
                .cast(pl.Enum(categories = ["arrival", "departure"]))
        )
        .select(Actual.column_names())
    )
    return


@app.cell
def _(pl):
    predicted = (
        pl.scan_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/RT_TRIP_UPDATES/year=2026/month=5/day=21/2026-05-21T00:00:00.parquet')
        .slice(1_000_000, 10_000)
        # .filter(pl.col("trip_update.trip.route_id").str.contains(r"^[1-9]")) # bus routes are numbered
        .with_columns(
            service_date = pl.col("trip_update.trip.start_date").str.strptime(dtype = pl.Date, format = "%Y%m%d"),
            trip_id = pl.col("trip_update.trip.trip_id"),
            vehicle_id = pl.col("trip_update.vehicle.id"),
            stop_sequence = pl.col("trip_update.stop_time_update.stop_sequence"),
            arrival = pl.col("trip_update.stop_time_update.arrival.time"),
            departure = pl.from_epoch("trip_update.stop_time_update.departure.time"),
            predicted_dt = pl.from_epoch("trip_update.timestamp")
        )
        # .filter(pl.col("service_date").eq(date))
        .unique(keep = "first")
        .unpivot(
            index = [
                "service_date",
                "trip_id",
                "vehicle_id",
                "stop_sequence",
            ],
            on = ["arrival", "departure"],
            value_name = "prediction_dt", 
            variable_name = "event_type"
        )
        .with_columns(
            event_type = pl.col("event_type").cast(pl.Enum(categories = ["arrival", "departure"])),
        )
    )
    return (predicted,)


@app.cell
def _(predicted):
    predicted.collect()
    return


if __name__ == "__main__":
    app.run()
