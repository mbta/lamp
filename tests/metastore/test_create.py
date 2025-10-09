import pytest
import duckdb

from lamp_py.metastore.create import create_read_date_partitioned

@pytest.mark.parametrize(
    ["dates"],
    [
        ("[DATE '2025-01-01']",),
        ("DATE '2025-01-01', DATE '2025-01-02'",),
    ],
    ids = ["date-list", "start-and-end-dates"]
)
def test_create_read_date_partitioned(dates: str) -> None:
    "It passes URLs with correct dates to DuckDB's read_parquet."
    create_read_date_partitioned()

    with pytest.raises(duckdb.HTTPException, match = r".*2025-01-01T00.*"):
        duckdb.sql(f"""
            SELECT *
            FROM read_date_partitioned('test', {dates})
        """)
