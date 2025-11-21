import os
from contextlib import nullcontext
from typing import Generator

import pytest
import duckdb

from lamp_py.publishing.lightswitch import build_view, add_views_to_local_metastore
from lamp_py.runtime_utils.remote_files import S3Location
from tests.test_resources import rt_vehicle_positions, tm_route_file


@pytest.fixture(name="duckdb_con")
def fixture_duckdb_con(
    path: str = ":memory:", read_only: bool = False
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    "Reusable DuckDB instance, scoped to each function."
    yield duckdb.connect(path, read_only)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


@pytest.mark.parametrize(
    ["partition_strategy", "data_location"],
    [
        ("", tm_route_file),
        ("/*/*/*/*/*.parquet", rt_vehicle_positions),
    ],
    ids=[
        "empty-partition-strategy",
        "nested-partition-strategy",
    ],
)
def test_build_view(duckdb_con: duckdb.DuckDBPyConnection, partition_strategy: str, data_location: S3Location) -> None:
    "It gracefully creates the view using the specified partition strategy."
    view_name = build_view(duckdb_con, "test", data_location, partition_strategy)
    view_exists = duckdb_con.sql(  # type: ignore[index]
        f"""
        SELECT count(*) FROM duckdb_views() WHERE view_name = '{view_name}' AND internal = false
    """
    ).fetchone()[0]
    assert view_exists == 1


# test if all views get built
@pytest.mark.parametrize(
    ["view_list", "view_names"],
    [
        (
            {"/*/*/*/*/*.parquet": [rt_vehicle_positions], "": [tm_route_file]},
            nullcontext(["RT_VEHICLE_POSITIONS", "TMMAIN_ROUTE"]),
        ),
        ({"fake_location": [rt_vehicle_positions]}, pytest.raises(Exception)),
    ],
    ids=[
        "valid",
        "invalid",
    ],
)
def test_add_views_to_local_metastore(
    duckdb_con: duckdb.DuckDBPyConnection, view_list: dict, view_names: pytest.RaisesExc
) -> None:
    "It builds the views that are passed to it."
    built_view_list = add_views_to_local_metastore(duckdb_con, view_list)
    with view_names:
        assert built_view_list == view_names.enter_result  # type: ignore[attr-defined]
