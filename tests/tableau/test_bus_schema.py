from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics
from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions
from lamp_py.tableau.jobs.bus_performance import bus_schema
import polars as pl


def test_bus_parquet_convertible_to_bus_tableau_columns() -> None:
    bus_df = bus_schema.empty_table()
    sample = BusPerformanceMetrics.sample(1)

    polars_df = pl.from_arrow(sample).select(bus_schema.names)

    excluded_columns = {
        "tm_planned_sequence_start",
    }
    # all columns in bus dataset should be in tableau dataset - just with type conversions
    assert set(sample.columns).difference(polars_df.columns) == excluded_columns

    check = apply_bus_analysis_conversions(polars_df)
    assert check.schema == bus_df.schema
