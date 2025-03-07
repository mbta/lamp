#!/usr/bin/env python

import polars as pl

from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions


# poetry run pytest -s tests/bus_performance_manager/test_bus_convert_for_tableau.py
def test_apply_bus_analysis_conversions() -> None:
    """
    Test extracted conversions for tableau user view
    """
    df = pl.read_parquet("tests/test_files/PUBLIC_ARCHIVE/lamp/bus_vehicle_events/test_events.parquet")
    table = apply_bus_analysis_conversions(polars_df=df)
    print(df)
    print(table)
