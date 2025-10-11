from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics
import polars as pl


def test_bus_headways() -> None:
    df = pl.read_parquet("bus_df.parquet")
    breakpoint()
    valid, invalid = BusPerformanceMetrics.filter(df)
    test_set = valid.filter(pl.col("trip_id").is_in(invalid.invalid()["trip_id"].unique().implode()))
    test_set.write_parquet("headway.parquet")
    print(invalid.cooccurrence_counts())
    print(valid.height)
    print(invalid.invalid().height)
    b = BusPerformanceMetrics.sample(1)  # , overrides={"route_direction_headway_seconds": -1})
    print(BusPerformanceMetrics.validate(b))

    print("EXPECT_FAIL")
    b = b.with_columns(pl.lit(-1).cast(pl.Int64).alias("route_direction_headway_seconds"))
    print(BusPerformanceMetrics.validate(b))

    # BusPerformanceMetrics.validate(b)

    # order_by=pl.coalesce("stop_departure_dt", "stop_arrival_dt", "plan_stop_departure_dt"),
