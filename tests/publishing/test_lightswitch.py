import os
from contextlib import nullcontext
from pathlib import Path
from typing import Generator

import polars as pl
import pytest
import duckdb

from lamp_py.publishing.lightswitch import (
    build_view,
    add_views_to_local_metastore,
    register_read_ymd,
)
from lamp_py.runtime_utils.remote_files import S3Location
from tests.test_resources import rt_vehicle_positions, tm_route_file


@pytest.fixture(name="duckdb_con")
def fixture_duckdb_con(
    path: str = ":memory:", read_only: bool = False
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Reusable DuckDB instance, scoped to each function."""
    yield duckdb.connect(path, read_only)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


@pytest.mark.parametrize(
    ["partition_strategy", "data_location", "result"],
    [
        ("", tm_route_file, True),
        ("/*/*/*/*/*.parquet", rt_vehicle_positions, True),
        ("fake_location", rt_vehicle_positions, False),
    ],
    ids=["empty-partition-strategy", "nested-partition-strategy", "invalid-location"],
)
def test_build_view(
    duckdb_con: duckdb.DuckDBPyConnection, partition_strategy: str, data_location: S3Location, result: pytest.RaisesExc
) -> None:
    """It gracefully creates the view using the specified partition strategy."""
    assert result == build_view(duckdb_con, "test", data_location, partition_strategy)


# test if all views get built
@pytest.mark.parametrize(
    ["view_dict", "view_names"],
    [
        (
            {"/*/*/*/*/*.parquet": [rt_vehicle_positions], "": [tm_route_file]},
            nullcontext(["RT_VEHICLE_POSITIONS", "TMMAIN_ROUTE"]),
        ),
        ({"fake location": [rt_vehicle_positions], "": [tm_route_file]}, nullcontext(["TMMAIN_ROUTE"])),
        ({"fake location": [rt_vehicle_positions]}, pytest.raises(Exception)),
    ],
    ids=[
        "valid",
        "1-valid",
        "all-invalid",
    ],
)
def test_add_views_to_local_metastore(
    duckdb_con: duckdb.DuckDBPyConnection, view_dict: dict, view_names: pytest.RaisesExc
) -> None:
    """It builds the views that are passed to it."""
    with view_names:
        assert view_names.enter_result == add_views_to_local_metastore(duckdb_con, view_dict)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ["directory_name", "raises"],
    [
        ("RT_VEHICLE_POSITIONS", nullcontext()),
        ("foo", pytest.raises(Exception)),
    ],
    ids=[
        "exists",
        "does-not-exist",
    ],
)
def test_register_read_ymd(
    duckdb_con: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    directory_name: str,
    raises: pytest.RaisesExc,
) -> None:
    """It raises errors on directories that don't exist."""
    file_path = tmp_path.joinpath("lamp/RT_VEHICLE_POSITIONS/year=2024/month=6/day=1/2024-06-01T00:00:00.parquet")
    file_path.parent.mkdir(parents=True)
    pl.read_parquet("tests/test_files/SPRINGBOARD/RT_VEHICLE_POSITIONS/year=2024").write_parquet(file_path)

    with raises:
        with duckdb_con:
            register_read_ymd(duckdb_con)
            assert duckdb_con.sql(
                f"SELECT * FROM read_ymd('{directory_name}', '2024-06-01' :: DATE, '2024-06-02' :: DATE, '{tmp_path.resolve()}')"
            )
