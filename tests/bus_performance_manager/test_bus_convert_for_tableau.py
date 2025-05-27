#!/usr/bin/env python

from unittest.mock import patch
import polars as pl

from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions


# poetry run pytest -s tests/bus_performance_manager/test_bus_convert_for_tableau.py
@patch("lamp_py.utils.filter_bank.GTFS_ARCHIVE_FILTER_BANK", "https://performancedata.mbta.com/lamp/gtfs_archive")
def test_apply_bus_analysis_conversions() -> None:
    """
    Test extracted conversions for tableau user view
    """
    df = pl.read_parquet("tests/test_files/PUBLIC_ARCHIVE/lamp/bus_vehicle_events/test_events.parquet")
    table = apply_bus_analysis_conversions(polars_df=df)
    print(df)
    print(table)
