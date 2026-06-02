import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        load aws;
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER credential_chain
        );

        -- this requires you to have the keys setup in your ~/.aws/credentials
        --
        """
    )
    return


@app.cell
def _(mo):
    vp1 = mo.sql(
        f"""
        select "vehicle.trip.trip_id",
            "vehicle.trip.route_id",
            "vehicle.current_stop_sequence",
            "vehicle.vehicle.id",
            "vehicle.timestamp",
            "vehicle.current_status",
            "feed_timestamp" from READ_PARQUET("~/Downloads/vp2026-05-21T00_00_00.parquet")
            where "vehicle.trip.route_id" NOT IN ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Mattapan') AND "vehicle.trip.route_id" NOT LIKE 'CR-';
        """
    )
    return (vp1,)


@app.cell
def _():
    TU_COLUMN_MAP = {
        "trip_update.trip.trip_id": "trip_id",
        "trip_update.stop_time_update.stop_sequence": "stop_sequence",
        "trip_update.stop_time_update.stop_id": "stop_id",
        "trip_update.vehicle.id": "vehicle_id",
        "trip_update.stop_time_update.arrival.time": "predicted_arrival",
        "trip_update.trip.route_id": "route_id",
        "trip_update.trip.start_time": "start_time",
        "feed_timestamp": "tu_feed_timestamp",
    }
    return (TU_COLUMN_MAP,)


@app.cell
def _(TU_COLUMN_MAP):
    list(TU_COLUMN_MAP.keys())
    return


app._unparsable_cell(
    r"""
        \"trip_update.trip.trip_id\",
        \"trip_update.stop_time_update.stop_sequence\",
        \"trip_update.stop_time_update.stop_id\",
        \"trip_update.vehicle.id\",
        \"trip_update.stop_time_update.arrival.time\",
        \"trip_update.trip.route_id\",
        \"trip_update.trip.start_time\",
        \"feed_timestamp\"
    """,
    name="_"
)


@app.cell
def _(mo):
    tu1 = mo.sql(
        f"""
        select     
            "trip_update.trip.trip_id",
            "trip_update.timestamp",
            "trip_update.stop_time_update.stop_sequence",
            "trip_update.stop_time_update.stop_id",
            "trip_update.vehicle.id",
            "trip_update.stop_time_update.arrival.time",
        	"trip_update.stop_time_update.departure.time",    
            "trip_update.trip.route_id",
            "trip_update.trip.start_time",
            "feed_timestamp" from READ_PARQUET("~/Downloads/tu2026-05-21T00_00_00.parquet") 
            where "trip_update.trip.route_id" NOT IN ('Red', 'Orange', 'Blue', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Mattapan') AND "trip_update.trip.route_id" NOT LIKE 'CR-%';
        """
    )
    return (tu1,)


@app.cell
def _(tu1):
    tu1.sort('trip_update.trip.route_id', 'trip_update.stop_time_update.stop_sequence').write_parquet('tu_20260521.parquet', compression_level=3)
    return


@app.cell
def _(vp1):
    vp1.write_parquet('vp_20260521.parquet', compression_level=3)
    return


@app.cell
def _(pl):
    vp = pl.read_parquet("vp_20260521.parquet")
    tu = pl.read_parquet("tu_20260521.parquet")
    # vp = vp1
    # tu = tu1
    return tu, vp


@app.cell
def _(vp):
    vp['vehicle.trip.trip_id'].unique()
    return


@app.cell
def _(tu):
    tu
    return


@app.cell
def _():
    # vp_narrow = vp.select("vehicle.trip.trip_id", "vehicle.current_stop_sequence", "vehicle.current_status", "vehicle.position.latitude", "vehicle.position.longitude", "vehicle.vehicle.id","vehicle.timestamp","feed_timestamp").filter(pl.col("vehicle.current_status") == "STOPPED_AT").unique(subset=["vehicle.trip.trip_id", "vehicle.current_stop_sequence"], keep="first").sort("vehicle.trip.trip_id", "vehicle.current_stop_sequence")
    return


@app.cell
def _():
    # joined = tu.join(vp_narrow, left_on=["trip_update.trip.trip_id", "trip_update.vehicle.id", "trip_update.stop_time_update.stop_sequence"], right_on=["vehicle.trip.trip_id", "vehicle.vehicle.id", "vehicle.current_stop_sequence"], how = "left")
    return


@app.cell
def _(joined):
    joined.columns
    return


@app.cell
def _():
    # ds = joined.with_columns(true_error=pl.col("trip_update.stop_time_update.arrival.time")- pl.col("vehicle.timestamp"), prediction_ahead=pl.col("feed_timestamp")-pl.col("trip_update.stop_time_update.arrival.time"))
    return


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


@app.cell
def _():
    from bus_prediction_analyzer_utils import AnalyzerConfig, add_error_columns, assign_ibi_bin, assign_time_of_day_bin, calculate_accuracy_by_group, calculate_ibi_accuracy, default_config, detect_prediction_bias, is_prediction_accurate, join_tu_vp, narrow_trip_updates, narrow_vehicle_positions, parse_start_time_seconds
    return (
        add_error_columns,
        assign_ibi_bin,
        assign_time_of_day_bin,
        calculate_accuracy_by_group,
        calculate_ibi_accuracy,
        default_config,
        is_prediction_accurate,
        join_tu_vp,
        narrow_trip_updates,
        narrow_vehicle_positions,
        parse_start_time_seconds,
    )


@app.cell
def _(pl, tu_narrow):
    tu_narrow.filter(pl.col("stop_sequence") == 3)
    return


@app.cell
def _():
    298
    return


app._unparsable_cell(
    r"""
    vp11 = 
    """,
    name="_"
)


@app.cell
def _(tu):
    tu['trip_update.trip.trip_id'].unique()
    return


@app.cell
def _(tu_narrow):
    tu_narrow
    return


@app.cell
def _(vp_narrow):
    vp_narrow
    return


@app.cell
def _(with_start_time):
    with_start_time
    return


@app.cell
def _(tu1):
    tu1
    return


@app.cell
def _(
    add_error_columns,
    assign_ibi_bin,
    assign_time_of_day_bin,
    calculate_accuracy_by_group,
    calculate_ibi_accuracy,
    default_config,
    detect_prediction_bias2,
    is_prediction_accurate,
    join_tu_vp,
    narrow_trip_updates,
    narrow_vehicle_positions,
    parse_start_time_seconds,
    pl,
    tu,
    vp,
):
    config = default_config()

    tu_narrow = narrow_trip_updates(tu.filter(pl.col('trip_update.trip.trip_id') == '75609175'))
    vp_narrow = narrow_vehicle_positions(vp.filter(pl.col('vehicle.trip.trip_id') == '75609175'))
    joined = join_tu_vp(tu_narrow, vp_narrow)
    # Step 3: Add error columns
    with_errors = add_error_columns(joined).filter(pl.col("prediction_ahead_sec")>-60*15).filter(pl.col("prediction_ahead_sec")<0)

    # Step 4-5: IBI binning and accuracy
    with_ibi_bin = assign_ibi_bin(with_errors, config)
    with_accuracy = is_prediction_accurate(with_ibi_bin, config)

    # Step 6: Overall IBI accuracy
    ibi_accuracy = calculate_ibi_accuracy(with_accuracy, config)

    # Step 7-8: Time-of-day binning and start_time parsing
    with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    with_start_time = parse_start_time_seconds(with_tod_bin)

    # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    route_accuracy = calculate_accuracy_by_group(with_start_time, ["route_id"], config)
    time_of_day_accuracy = calculate_accuracy_by_group(
        with_start_time, ["time_of_day_bin"], config
    )

    # Step 10: Prediction bias
    bias = detect_prediction_bias2(with_start_time)
    # # Step 3: Add error columns
    return bias, ibi_accuracy, joined, tu_narrow, vp_narrow, with_start_time


@app.cell
def _(tu_narrow):
    tu_narrow
    return


@app.cell
def _(VP_COLUMN_MAP, pl):

    def narrow_vehicle_positions2(df: pl.DataFrame) -> pl.DataFrame:
        """Filter and deduplicate vehicle positions to one row per stop visit.

        Filters to STOPPED_AT status, deduplicates by (trip_id, stop_sequence)
        keeping the first occurrence, renames to canonical column names, and sorts.
        """
        required = list(VP_COLUMN_MAP.keys())
        return (
            df.select(required)
            .filter(pl.col("vehicle.current_status") == "STOPPED_AT")
            .unique(subset=["vehicle.trip.trip_id", "vehicle.current_stop_sequence"], keep="first")
            .sort("vehicle.trip.trip_id", "vehicle.current_stop_sequence")
            .rename(VP_COLUMN_MAP)
            .drop("current_status")
        )
    return


@app.cell
def _(pl, with_start_time):
    raw = with_start_time.filter(pl.col("trip_id") == "75255757").sort("stop_sequence").with_columns(prediction_ahead_mins=pl.col("prediction_ahead_sec")/60)
    return (raw,)


@app.cell
def _(pl, raw):
    raw.filter(pl.col("is_accurate").is_null())
    return


@app.cell
def _(ibi_accuracy):
    ibi_accuracy
    return


@app.cell
def _(bias):
    bias
    return


@app.cell
def _(pl):

    def detect_prediction_bias2(
        df: pl.DataFrame,
        bias_threshold_sec: float = 30.0,
        consistency_threshold_sec: float = 20.0,
    ) -> pl.DataFrame:
        """Detect consistent directional bias per trip.

        Computes per-trip mean and std of ``prediction_error_sec``. Flags trips as biased
        when ``|mean_error| > bias_threshold_sec`` AND ``std_error < consistency_threshold_sec``
        (indicating consistent direction without adjustment).

        Args:
            df: DataFrame with trip_id and prediction_error_sec columns.
            bias_threshold_sec: Minimum |mean_error| to consider as biased (default 30.0).
            consistency_threshold_sec: Maximum std for consistent behavior (default 20.0).

        Returns:
            DataFrame with columns: trip_id, mean_error, std_error, is_biased.
        """
        trips_with_errors = df.filter(pl.col("prediction_error_sec").is_not_null())
        if trips_with_errors.is_empty():
            return pl.DataFrame(
                schema={
                    "trip_id": pl.Utf8,
                    "mean_error": pl.Float64,
                    "std_error": pl.Float64,
                    "is_biased": pl.Boolean,
                }
            )

        result = (
            trips_with_errors.group_by("trip_id")
            .agg(
                mean_error=pl.col("prediction_error_sec").mean(),
                median_error=pl.col("prediction_error_sec").median(),
                mode_error=pl.col("prediction_error_sec").mode(),
                std_error=pl.col("prediction_error_sec").std(),
            )
            .with_columns(
                std_error=pl.col("std_error").fill_null(0.0)  # Single value -> std=NaN becomes 0
            )
            .with_columns(
                is_biased=(
                    (pl.col("mean_error").abs() > bias_threshold_sec)
                    & (pl.col("std_error") < consistency_threshold_sec)
                )
            )
            .sort("trip_id")
        )
        return result
    return (detect_prediction_bias2,)


if __name__ == "__main__":
    app.run()
