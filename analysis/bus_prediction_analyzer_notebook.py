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
    _df = mo.sql(
        f"""
        select * from READ_PARQUET("~/Downloads/vp2026-05-21T00_00_00.parquet") limit 1;
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
            "vehicle.position.longitude",
        	"vehicle.position.latitude",
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
        "trip_update.stop_time_update.arrival.time": "predicted_arrival_time",
        "trip_update.trip.route_id": "route_id",
        "trip_update.trip.start_time": "start_time",
        "trip_update.timestamp": "tu_last_update_timestamp",
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
def _(vp):
    vp
    return


@app.cell
def _(pl):
    vp = pl.scan_parquet("vp_20260521.parquet")
    # vp = (
    #         pl.scan_parquet('~/Downloads/20260521.parquet')
    #         .filter(
    #             pl.col("vehicle_label").is_not_null(),
    #             pl.col("gtfs_stop_sequence").is_not_null(),
    #             pl.col("gtfs_arrival_dt").is_not_null()
    #         )
    #         .with_columns(
    #             vehicle_id = pl.col("vehicle_label"),
    #             stop_sequence = pl.col("gtfs_stop_sequence"),
    #         )
    #         .with_columns(
    #             # event_type = pl.lit("arrival").cast(pl.Enum(categories = ["arrival", "departure"])),
    #             vp_last_update_timestamp = pl.col("gtfs_arrival_dt").dt.timestamp('ms')/1000
    #         )
    #         .select("trip_id", "stop_sequence", "vehicle_id", "latitude", "longitude", "vp_last_update_timestamp")
    #     )
    #     # return (actual,)
    tu = pl.scan_parquet("tu_20260521.parquet")
    # vp = vp1
    # tu = tu1
    return tu, vp


@app.cell
def _(pl):
    VP_COLUMN_MAP = {
        "vehicle.trip.trip_id": "trip_id",
        "vehicle.current_stop_sequence": "stop_sequence",
        "vehicle.vehicle.id": "vehicle_id",
        "vehicle.timestamp": "vp_last_update_timestamp",
        "vehicle.current_status": "current_status",
        "vehicle.position.longitude": "longitude",
        "vehicle.position.latitude": "latitude",
        "feed_timestamp": "vp_feed_timestamp",
    }
    def narrow_vehicle_positions2(df: pl.LazyFrame) -> pl.LazyFrame:
        """Filter and deduplicate vehicle positions to one row per stop visit.

        Filters to STOPPED_AT status, deduplicates by (trip_id, stop_sequence)
        keeping the first occurrence, renames to canonical column names, and sorts.
        """
        required = list(VP_COLUMN_MAP.keys())
        return (
            df.select(required)
            # .filter(pl.col("vehicle.current_status") == "STOPPED_AT")
            # median here? agg it? get median vehicle position stoppped at. 
            .unique(subset=["vehicle.trip.trip_id", "vehicle.current_stop_sequence"], keep="last")
            .sort("vehicle.trip.trip_id", "vehicle.current_stop_sequence")
            .rename(VP_COLUMN_MAP)
            # .drop("current_status")
        )
    return (narrow_vehicle_positions2,)


@app.cell
def _(narrow_vehicle_positions2, pl, vp):
    vp2 = narrow_vehicle_positions2(vp).filter(pl.col("trip_id") == "75609175").collect()
    return (vp2,)


@app.cell
def _(vp2):
    vp2
    return


@app.cell
def _():
    from prism import plot_lla
    return (plot_lla,)


@app.cell
def _(plot_lla, vp2):
    plot_lla(vp2)
    return


@app.cell
def _(pl):

    # Earth's radius in meters
    EARTH_RADIUS_M = 6_371_000

    def haversine_distance(
        lat1: pl.Expr, lon1: pl.Expr, 
        lat2: pl.Expr, lon2: pl.Expr
    ) -> pl.Expr:
        """Calculate distance in meters between two lat/lon points."""
        # Convert to radians
        lat1_rad = lat1.radians()
        lat2_rad = lat2.radians()
        lon1_rad = lon1.radians()
        lon2_rad = lon2.radians()

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (dlat / 2).sin().pow(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2).sin().pow(2)
        c = 2 * a.sqrt().arctan2((1 - a).sqrt())

        return c * EARTH_RADIUS_M
    return


@app.cell
def _(pl, vp2):
    R = 6371000

    vp2.sort("vp_feed_timestamp").with_columns(
        # Convert degrees to radians
        start_lat_rad = pl.col("latitude") * (3.1415 / 180),
        start_lon_rad = pl.col("longitude") * (3.1415 / 180),
        end_lat_rad = pl.col("latitude").shift() * (3.1415 / 180),
        end_lon_rad = pl.col("longitude").shift() * (3.1415 / 180)
    ).with_columns(
        d_lat = pl.col("end_lat_rad") - pl.col("start_lat_rad"),
        d_lon = pl.col("end_lon_rad") - pl.col("start_lon_rad")
    ).with_columns(
        # Haversine formula
        a = (pl.col("d_lat") / 2).sin().pow(2) + 
            pl.col("start_lat_rad").cos() * pl.col("end_lat_rad").cos() * (pl.col("d_lon") / 2).sin().pow(2)
    ).with_columns(
        c = pl.arctan2(2 * pl.col("a").sqrt(), (1 - pl.col("a")).sqrt())
        # Angular distance in radians

    ).with_columns(
        # Final distance
        distance_m = R * pl.col("c")
    ).drop("start_lat_rad", "start_lon_rad", "end_lat_rad", "end_lon_rad", "d_lat", "d_lon", "a", "c").with_columns(
        time_to_next_stop = pl.col("vp_last_update_timestamp") - pl.col("vp_last_update_timestamp").shift() 
    )
    return


@app.cell
def _(make_prediction_timeline_chart, predictions_df):
    fig = make_prediction_timeline_chart(
        predictions_df,
        position_col="stop_sequence",
        prediction_time_col="prediction_timestamp",  # or "prediction_ahead_meas_sec"
        accurate_col="is_accurate",
    )
    fig.show()
    return


@app.cell
def _(vp):
    vp.collect()
    return


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
        calculate_ibi_accuracy,
        default_config,
        is_prediction_accurate,
        join_tu_vp,
        narrow_trip_updates,
        narrow_vehicle_positions,
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
def _():
    from ibi_chart import make_benchmark_chart
    return (make_benchmark_chart,)


@app.cell
def _(with_start_time):
    with_start_time
    return


@app.cell
def _(tu):
    tu
    return


@app.cell
def _(with_accuracy):
    with_accuracy
    return


@app.cell
def _():
    ibi_dict = {}
    return


@app.cell
def _():
    trip_id  = 75609175
    return


@app.cell
def _(tu):
    route_ids = tu['trip_update.trip.route_id'].unique()
    return (route_ids,)


@app.cell
def _(route_ids):
    route_ids
    return


@app.cell
def _():
    75609175
    return


@app.cell
def _(narrow_trip_updates, tu):
    tu_tid = narrow_trip_updates(tu).select("trip_id").unique().collect()
    return (tu_tid,)


@app.cell
def _(pl, vp):
    vp.filter(pl.col("trip_id") == "75609175").collect()
    return


@app.cell
def _(tu_narrow):
    tu_narrow.collect()
    return


@app.cell
def _(
    add_error_columns,
    assign_ibi_bin,
    calculate_ibi_accuracy,
    default_config,
    is_prediction_accurate,
    join_tu_vp,
    narrow_trip_updates,
    pl,
    tu,
    vp,
):
    # narrow_trip_updates(tu).collect()

    config = default_config()


    tu_narrow = narrow_trip_updates(tu)
    # vp_narrow = narrow_vehicle_positions(vp).group_by("trip_id", "vehicle_id","stop_sequence").agg(vp_last_update_timestamp=pl.col("vp_last_update_timestamp").median(), vp_feed_timestamp=pl.col("vp_feed_timestamp").median()).sort("stop_sequence")

    joined = join_tu_vp(tu_narrow, vp)
    # # Step 3: Add error columns
    with_errors = add_error_columns(joined).filter(pl.col("prediction_ahead_meas_sec")>-60*15).filter(pl.col("prediction_ahead_meas_sec")<0)

    # # Step 4-5: IBI binning and accuracy
    with_ibi_bin = assign_ibi_bin(with_errors, config, "prediction_ahead_meas_sec")
    with_accuracy2 = is_prediction_accurate(with_ibi_bin, config, error_column="prediction_error_meas_sec")

    # # Step 7-8: Time-of-day binning and start_time parsing
    # with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    # with_start_time = parse_start_time_seconds(with_tod_bin)

    ## Agg
    # Step 6: Overall IBI accuracy
    ibi_accuracy = calculate_ibi_accuracy(with_accuracy2, config)

    # # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    # route_accuracy = calculate_accuracy_by_group(with_accuracy2, ["route_id"], config)
    # # time_of_day_accuracy = calculate_accuracy_by_group(
    # #     with_start_time, ["time_of_day_bin"], config
    # # )

    # # # Step 10: Prediction bias
    # bias = detect_prediction_bias2(with_accuracy2)
    # make_benchmark_chart(predictions_df=with_accuracy2)
    # result = pl.concat([route_accuracy.drop("accuracy_pct"), ibi_accuracy.drop("total", "accurate"), bias], how="horizontal")
    # # print(ibi_accuracy)
    #     # return with_accuracy2
    # # abc = _("75609175")
    # abc = with_accuracy2
    return ibi_accuracy, joined, tu_narrow, with_accuracy2


@app.cell
def _(joined, pl):
    joined.filter(pl.col("trip_id") == "75609175").collect()
    return


@app.cell
def _(ibi_accuracy):
    all_ibi = ibi_accuracy.collect()
    return (all_ibi,)


@app.cell
def _(all_ibi):
    all_ibi
    return


@app.cell
def _():
    16907676
    return


@app.cell
def _(all_ibi):
    all_ibi
    return


@app.cell
def _(tu_tid):
    tu_tid.to_series()
    return


@app.cell
def _(pl, tu_tid):
    tu_tid.filter(~pl.col("trip_id").str.starts_with("7"))
    return


@app.cell
def _(narrow_vehicle_positions, pl, vp):
    narrow_vehicle_positions(vp).filter(pl.col("trip_id") == "75179356_1")
    return


@app.cell
def _(narrow_vehicle_positions, vp):
    narrow_vehicle_positions(vp)
    return


@app.cell
def _(narrow_vehicle_positions, pl, vp):
    a = narrow_vehicle_positions(vp).group_by("trip_id", "vehicle_id", "stop_sequence").agg(vp_last_update_timestamp=pl.col("vp_last_update_timestamp").median(), vp_feed_timestamp=pl.col("vp_feed_timestamp").median()).sort("stop_sequence")
    return (a,)


@app.cell
def _(a, pl, tu_tid):
    a.collect().filter(pl.col("trip_id").is_in(tu_tid.to_series().to_list()))
    return


app._unparsable_cell(
    r"""
    distance x...stop_sequence_y, 

    # at first position, lost of predictions for all of stops - as position moves, predictions for stops changes...
    """,
    name="_"
)


@app.cell
def _():
    # this_trip_id = "75609175"
    # # def _(this_trip_id):

    # config = default_config()

    # tu_narrow = narrow_trip_updates(tu.filter(pl.col('trip_update.trip.trip_id') == this_trip_id))
    # vp_narrow = narrow_vehicle_positions(vp.filter(pl.col('vehicle.trip.trip_id') == this_trip_id))
    # joined = join_tu_vp(tu_narrow, vp_narrow)
    # # # Step 3: Add error columns
    # with_errors = add_error_columns(joined).filter(pl.col("prediction_ahead_meas_sec")>-60*15).filter(pl.col("prediction_ahead_meas_sec")<0)

    # # # Step 4-5: IBI binning and accuracy
    # with_ibi_bin = assign_ibi_bin(with_errors, config, "prediction_ahead_meas_sec")
    # with_accuracy2 = is_prediction_accurate(with_ibi_bin, config, error_column="prediction_error_meas_sec")

    # # # Step 7-8: Time-of-day binning and start_time parsing
    # # with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    # # with_start_time = parse_start_time_seconds(with_tod_bin)

    # ## Agg
    # # Step 6: Overall IBI accuracy
    # ibi_accuracy = calculate_ibi_accuracy(with_accuracy2, config)

    # # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    # route_accuracy = calculate_accuracy_by_group(with_accuracy2, ["route_id"], config)
    # # time_of_day_accuracy = calculate_accuracy_by_group(
    # #     with_start_time, ["time_of_day_bin"], config
    # # )

    # # # Step 10: Prediction bias
    # bias = detect_prediction_bias2(with_accuracy2)
    # make_benchmark_chart(predictions_df=with_accuracy2)
    # result = pl.concat([route_accuracy.drop("accuracy_pct"), ibi_accuracy.drop("total", "accurate"), bias], how="horizontal")
    # # print(ibi_accuracy)
    #     # return with_accuracy2
    # # abc = _("75609175")
    # abc = with_accuracy2
    return


@app.cell
def _(with_accuracy2):
    with_accuracy2.unique(subset=["predicted_arrival_time", "vp_last_update_timestamp"]).sort("predicted_arrival_time", "vp_last_update_timestamp", "stop_sequence")
    return


@app.cell
def _(abc, pl):
    abc.filter(pl.col("stop_sequence") == 3)
    return


@app.cell
def _(abc, make_benchmark_chart):
    make_benchmark_chart(predictions_df=abc)
    return


@app.cell
def _(abc, pl):
    abc.sort("stop_sequence", "tu_feed_timestamp").filter(pl.col("vp_last_update_timestamp").is_not_null()).unique(subset=["vp_feed_timestamp", "ibi_bin"], keep="last", maintain_order=True).select("stop_sequence", "prediction_error_meas_sec", "prediction_error_feed_sec", "prediction_ahead_meas_sec", "prediction_ahead_feed_sec", "ibi_bin", "is_accurate").sort(["stop_sequence", "ibi_bin"], descending=[False, True])
    return


@app.cell
def _(ibi_accuracys):
    ibi_accuracys
    return


@app.cell
def _(ibi_accuracys, pl):
    ibis = pl.concat(ibi_accuracys)
    return (ibis,)


@app.cell
def _(ibis):
    ibis.sort('accuracy_pct')
    return


@app.cell
def _(ibis, pl):
    ibis.filter(pl.col.route_id == "18")
    return


@app.cell
def _(ibis):
    ibis.write_parquet('all_ibi.parquet')
    return


@app.cell
def _(biases):
    biases
    return


@app.cell
def _():
    ibi_accuracys = []
    biases = []
    return biases, ibi_accuracys


@app.cell
def _(tu):
    trip_ids = tu['trip_update.trip.trip_id'].unique()
    return


@app.cell
def _():
    # config = default_config()

    # for route_id in trip_ids:
    #     print(route_id)
    #     tu_narrow = narrow_trip_updates(tu.filter(pl.col('trip_update.trip.trip_id') == route_id))
    #     vp_narrow = narrow_vehicle_positions(vp.filter(pl.col('vehicle.trip.trip_id') == route_id))
    #     joined = join_tu_vp(tu_narrow, vp_narrow)
    #     # # Step 3: Add error columns
    #     with_errors = add_error_columns(joined).filter(pl.col("prediction_ahead_meas_sec")>-60*15).filter(pl.col("prediction_ahead_meas_sec")<0)

    #     # # Step 4-5: IBI binning and accuracy
    #     with_ibi_bin = assign_ibi_bin(with_errors, config, "prediction_ahead_meas_sec")
    #     with_accuracy = is_prediction_accurate(with_ibi_bin, config, error_column="prediction_error_meas_sec")
    #     with_accuracy.write_parquet(f"/Users/hhuang/trips/{route_id}.parquet")
    #     # # Step 7-8: Time-of-day binning and start_time parsing
    #     # with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    #     # with_start_time = parse_start_time_seconds(with_tod_bin)

    #     ### Agg
    #     # # Step 6: Overall IBI accuracy
    #     ibi_accuracys.append(calculate_ibi_accuracy(with_accuracy, config).with_columns(pl.lit(route_id).alias("trip_id")))

    #     # # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    #     # route_accuracy = calculate_accuracy_by_group(ibi_accuracy, ["route_id"], config)
    #     # time_of_day_accuracy = calculate_accuracy_by_group(
    #     #     with_start_time, ["time_of_day_bin"], config
    #     # )

    #     # # Step 10: Prediction bias
    #     # biases.append(detect_prediction_bias2(with_accuracy))

    # # result = pl.concat([route_accuracy.drop("accuracy_pct"), ibi_accuracy.drop("total", "accurate"), bias], how="horizontal")
    return


@app.cell
def _(ibi_accuracy):
    ibi_accuracy
    return


@app.cell
def _(with_accuracy):
    with_accuracy
    return


@app.cell
def _(make_benchmark_chart, with_accuracy):
    make_benchmark_chart(predictions_df=with_accuracy)
    return


@app.cell
def _(result):
    result
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
def _(tu_narrow):
    tu_narrow
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
        trips_with_errors = df.filter(pl.col("prediction_error_meas_sec").is_not_null())
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
                mean_error=pl.col("prediction_error_meas_sec").mean(),
                median_error=pl.col("prediction_error_meas_sec").median(),
                mode_error=pl.col("prediction_error_meas_sec").mode(),
                std_error=pl.col("prediction_error_meas_sec").std(),
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
    return


@app.cell
def _():
    from ibi_chart import make_prediction_timeline_chart
    return (make_prediction_timeline_chart,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
