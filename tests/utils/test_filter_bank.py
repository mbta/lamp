from datetime import datetime
import itertools
from typing import Optional
from unittest.mock import patch
import polars as pl

from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter

# list files
# grab latest
# assert hardcodes still hold
# get latest


def test_hardcoded_terminal_prediction_names() -> None:
    # the stops listed for these filters are retrieved dynamically from gtfs.
    # ensure that the expected list contains all of the expected terminal values

    # associated runner:
    # runners/run_gtfs_rt_parquet_converter.py

    def list_station_child_stops_from_gtfs(
        stops: pl.DataFrame, parent_station: str, additional_filter: Optional[pl.Expr] = None
    ) -> pl.DataFrame:
        """
        Filter gtfs stops by parent_station string, and additional filter if available
        """
        df_parent_station = stops.filter(pl.col("parent_station") == parent_station)
        if additional_filter is not None:
            df_parent_station = df_parent_station.filter(additional_filter)
        return df_parent_station

    terminal_stop_ids = []
    heavy_rail_filter = pl.col("vehicle_type") == 1

    # check that all stops in Filter lists exist
    service_date = datetime.now()
    stops = pl.read_parquet(f"https://performancedata.mbta.com/lamp/gtfs_archive/{service_date.year}/stops.parquet")

    for place_name in HeavyRailFilter._terminal_stop_place_names:
        gtfs_stops = list_station_child_stops_from_gtfs(stops, place_name, heavy_rail_filter)
        terminal_stop_ids.extend(gtfs_stops["stop_id"].to_list())

    assert set(terminal_stop_ids).issuperset(set(HeavyRailFilter.terminal_stop_ids))

    for stop in HeavyRailFilter.terminal_stop_ids:
        bb = stops.filter(pl.col("stop_id") == stop)
        assert stops.filter(pl.col("stop_id") == stop).height == 1

    for stop in LightRailFilter.terminal_stop_ids:
        bb = stops.filter(pl.col("stop_id") == stop)
        assert stops.filter(pl.col("stop_id") == stop).height == 1
