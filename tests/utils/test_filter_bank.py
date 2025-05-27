from datetime import datetime
import itertools
from unittest.mock import patch
import polars as pl

from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter

# list files
# grab latest
# assert hardcodes still hold
# get latest


@patch("lamp_py.utils.filter_bank.GTFS_ARCHIVE", "https://performancedata.mbta.com/lamp/gtfs_archive")
def test_hardcoded_terminal_prediction_names() -> None:
    # the stops listed for these filters are retrieved dynamically from gtfs.
    # ensure that the expected list contains all of the expected terminal values

    # associated runner:
    # runners/run_gtfs_rt_parquet_converter.py

    forest_hills_stop_id = ["70001", "Forest Hills-01", "Forest Hills-02"]
    oak_grove_stop_id = ["70036", "Oak Grove-01", "Oak Grove-02"]
    bowdoin_stop_id = ["70038"]
    wonderland_stop_id = ["70059"]
    alewife_stop_id = ["70061", "Alewife-01", "Alewife-02"]
    ashmont_stop_id = ["70094"]
    braintree_stop_id = ["70105", "Braintree-01", "Braintree-02"]

    terminal_stop_ids_list = list(
        itertools.chain(
            forest_hills_stop_id,
            oak_grove_stop_id,
            bowdoin_stop_id,
            wonderland_stop_id,
            alewife_stop_id,
            ashmont_stop_id,
            braintree_stop_id,
        )
    )

    assert set(HeavyRailFilter.terminal_stop_ids).issuperset(set(terminal_stop_ids_list))

    # check that all stops in Filter lists exist
    service_date = datetime.now()
    stops = pl.read_parquet(f"https://performancedata.mbta.com/lamp/gtfs_archive/{service_date.year}/stops.parquet")

    for stop in HeavyRailFilter.terminal_stop_ids:
        bb = stops.filter(pl.col("stop_id") == stop)
        assert stops.filter(pl.col("stop_id") == stop).height == 1

    for stop in LightRailFilter.terminal_stop_ids:
        bb = stops.filter(pl.col("stop_id") == stop)
        assert stops.filter(pl.col("stop_id") == stop).height == 1
