# from lamp_py. l1_cte_statements import (
#     static_trips_subquery,
# )
import polars as pl
from polars.testing import assert_frame_equal, assert_series_equal
from lamp_py.performance_manager.l1_cte_statements import static_trips_subquery_pl


def test_static_trips_subquery_pl() -> None:
    """
    Passing unit test for static_trips_subquery implementation in polars/parquet
    """
    # ┌─────────────────────────┬──────────────┬───────────────────┬───────────────────┬────────────────┐
    # │ static_trip_id          ┆ direction_id ┆ static_stop_count ┆ static_start_time ┆ route_id       │
    # │ ---                     ┆ ---          ┆ ---               ┆ ---               ┆ ---            │
    # │ str                     ┆ i64          ┆ u32               ┆ str               ┆ str            │
    # ╞═════════════════════════╪══════════════╪═══════════════════╪═══════════════════╪════════════════╡
    static_trips_sub_res = static_trips_subquery_pl(20250410).sort(by="static_trip_id")

    # ┌─────────────────────────┬────────────────┬──────────────┬───────────────────┬───────────────────┐
    # │ static_trip_id          ┆ route_id       ┆ direction_id ┆ static_start_time ┆ static_stop_count │
    # │ ---                     ┆ ---            ┆ ---          ┆ ---               ┆ ---               │
    # │ str                     ┆ str            ┆ str          ┆ str               ┆ str               │
    # ╞═════════════════════════╪════════════════╪══════════════╪═══════════════════╪═══════════════════╡
    compare_sql = pl.read_csv(
        "tests/test_files/replace_perf_mgr_query_test_data/staging_test_summary_sub.csv", infer_schema=False
    )

    # line up the types - convert the 24+ hour timestamp to math-able values
    static_trips_pl = static_trips_sub_res.with_columns(
        pl.col("static_start_time")
        .str.splitn(":", 3)
        .struct.rename_fields(["hour", "minute", "second"])
        .alias("fields")
    ).unnest("fields")
    static_trips_sql = compare_sql.with_columns(
        pl.col("static_stop_count").cast(pl.UInt32),
        pl.col("static_start_time").cast(pl.Int32),
        pl.when(pl.col("direction_id") == "f")
        .then(pl.lit(0))
        .otherwise(pl.lit(1))
        .alias("direction_id")
        .cast(pl.Int64),
    )

    # assert against test csv for all rows
    # all the easy ones
    assert_frame_equal(
        static_trips_pl.select(["static_trip_id", "direction_id", "static_stop_count", "route_id"]),
        static_trips_sql.select(["static_trip_id", "direction_id", "static_stop_count", "route_id"]),
    )

    # now the timestamp...
    tmp = static_trips_pl.select(["hour", "minute", "second"]).cast(pl.Int32)
    assert_series_equal(
        tmp["hour"] * 60 * 60 + tmp["minute"] * 60 + tmp["second"],
        static_trips_sql["static_start_time"],
        check_names=False,
    )


# def insert_dataframe(self, dataframe: pandas.DataFrame, insert_table: Any) -> None:
# """
# insert data into db table from pandas dataframe
# """
# insert_as = self._get_schema_table(insert_table)

# with self.session.begin() as cursor:
#     cursor.execute(
#         sa.insert(insert_as),
#         dataframe.to_dict(orient="records"),
#     )
