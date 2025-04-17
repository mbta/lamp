import pytest
import polars as pl
from unittest.mock import patch
from polars.testing import assert_frame_equal
from lamp_py.performance_manager.l1_cte_statements import static_trips_subquery_pl


@patch(
    "lamp_py.performance_manager.l1_cte_statements.GTFS_ARCHIVE", "https://performancedata.mbta.com/lamp/gtfs_archive"
)
def test_static_trips_subquery_pl() -> None:
    """
    Passing unit test for static_trips_subquery implementation in polars/parquet
    """

    static_trips_pl = static_trips_subquery_pl(20250410).sort(by="static_trip_id")

    compare_sql = pl.read_csv(
        "tests/test_files/replace_perf_mgr_query_test_data/staging_test_summary_sub.csv", infer_schema=False
    )

    # need to do a few things because the csv output doesn't do types well
    static_trips_sql = compare_sql.with_columns(
        pl.col("static_stop_count").cast(pl.Int16),
        pl.col("static_start_time").cast(pl.Int32),
        pl.when(pl.col("direction_id") == "f").then(pl.lit(False)).otherwise(pl.lit(True)).alias("direction_id"),
    )

    # assert against test csv for all rows
    assert_frame_equal(static_trips_pl, static_trips_sql, check_column_order=False)
