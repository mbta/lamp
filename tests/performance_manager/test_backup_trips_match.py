import pytest
import polars as pl
from unittest.mock import patch
from lamp_py.performance_manager.l1_cte_statements import static_trips_subquery_pl
from lamp_py.performance_manager.l1_rt_trips import backup_trips_match_pl


@patch(
    "lamp_py.performance_manager.l1_cte_statements.GTFS_ARCHIVE", "https://performancedata.mbta.com/lamp/gtfs_archive"
)
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
    rt_trips = rt_trips_raw.with_columns(
        pl.when(pl.col("direction_id") == "f").then(pl.lit(False)).otherwise(pl.lit(True)).alias("direction_id"),
        pl.col("start_time").cast(pl.Int32).alias("start_time"),
    )

    static_trips = static_trips_subquery_pl(20250415)
    backup_matched_trips = backup_trips_match_pl(rt_trips, static_trips)
