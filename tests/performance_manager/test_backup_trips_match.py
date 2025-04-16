import polars as pl
from lamp_py.performance_manager.l1_cte_statements import static_trips_subquery_pl
from lamp_py.performance_manager.l1_rt_trips import backup_trips_match_pl


def test_backup_trips_match() -> None:
    """
    test backup_trips_match
    """
    # ┌─────────────────────────┬──────────────┬───────────────────┬───────────────────┬────────────────┐
    # │ static_trip_id          ┆ direction_id ┆ static_stop_count ┆ static_start_time ┆ route_id       │
    # │ ---                     ┆ ---          ┆ ---               ┆ ---               ┆ ---            │
    # │ str                     ┆ i64          ┆ u32               ┆ str               ┆ str            │
    # ╞═════════════════════════╪══════════════╪═══════════════════╪═══════════════════╪════════════════╡
    rt_trips_raw = pl.read_csv(
        "tests/test_files/replace_perf_mgr_query_test_data/20250415_rt_trips_for_backup_match_subquery.csv",
        infer_schema=False,
    )
    # static_trips = pl.read_csv('tests/test_files/replace_perf_mgr_query_test_data/20250415_static_trips_subquery.csv', infer_schema=False)
    rt_trips = rt_trips_raw.with_columns(
        pl.when(pl.col("direction_id") == "f")
        .then(pl.lit(0))
        .otherwise(pl.lit(1))
        .alias("direction_id")
        .cast(pl.Int64),
        pl.col("start_time").cast(pl.Int64).alias("start_time_int"),
    )
    rt_trips = rt_trips.with_columns(
        (pl.col("start_time_int") / (60 * 60)).floor().cast(pl.Int64).alias("hh"),
        (pl.col("start_time_int").mod(3600) / 60).floor().cast(pl.Int64).alias("mm"),
        (pl.col("start_time_int").mod(3600).mod(60)).floor().cast(pl.Int64).alias("ss"),
    )
    # breakpoint()
    rt_trips = rt_trips.with_columns(
        pl.duration(hours=pl.col("hh"), minutes=pl.col("mm"), seconds=pl.col("ss")).alias("start_time")
    )
    # breakpoint()
    # rt_trips = rt_trips.with_columns()
    # rt_trips = rt_trips.with_columns((pl.col("start_time_int")/(60*60)).floor().cast(pl.Int64).cast(pl.String).alias("hh"))
    # rt_trips2 = rt_trips.with_columns((pl.col("start_time_int").mod(3600)/60).floor().cast(pl.Int64).cast(pl.String).alias("mm"))
    # rt_trips3 = rt_trips2.with_columns((pl.col("start_time_int").mod(3600).mod(60)).floor().cast(pl.Int64).cast(pl.String).alias("ss"))

    #  ((pl.col("start_time_int").mod(3600)/60).floor().cast(pl.Int64).cast(pl.String).alias("mm")),
    #                             (((pl.col("start_time_int").mod(3600)/60).mod(60)).floor().cast(pl.Int64).cast(pl.String).alias("ss"))

    static_trips = static_trips_subquery_pl(20250415)
    # breakpoint()
    backup_matched_trips = backup_trips_match_pl(rt_trips, static_trips)

    print(backup_matched_trips)
    # is it going to be strings IRL? What is the datatype of this stuff when it comes back


# update_query = (
#     sa.update(VehicleTrips.__table__)
#     .where(
#         VehicleTrips.pm_trip_id == backup_trips_match.c.pm_trip_id,
#     )
#     .values(
#         static_trip_id_guess=backup_trips_match.c.static_trip_id,
#         static_start_time=backup_trips_match.c.static_start_time,
#         static_stop_count=backup_trips_match.c.static_stop_count,
#         first_last_station_match=backup_trips_match.c.first_last_station_match,
#     )
# )
