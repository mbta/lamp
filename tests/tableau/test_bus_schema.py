from lamp_py.bus_performance_manager.events_metrics import BusPerformanceMetrics
from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions
from lamp_py.tableau.jobs.bus_performance import bus_schema
import polars as pl
import pyarrow


def test_bus_parquet_convertable_to_bus_tableau_columns() -> None:
    bus_df = bus_schema.empty_table()
    sample = BusPerformanceMetrics.sample(1)

    polars_df = pl.from_arrow(sample).select(bus_schema.names)
    check = apply_bus_analysis_conversions(polars_df)

    assert check.schema == bus_df.schema
