import os
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
    "It gracefully creates the view using the specified partition strategy."
    assert result == build_view(duckdb_con, "test", data_location, partition_strategy)


# test if all views get built
@pytest.mark.parametrize(
    ["view_dict", "view_names"],
    [
        (
            {"/*/*/*/*/*.parquet": [rt_vehicle_positions], "": [tm_route_file]},
            ["RT_VEHICLE_POSITIONS", "TMMAIN_ROUTE"],
        ),
        ({"fake location": [rt_vehicle_positions], "": [tm_route_file]}, ["TMMAIN_ROUTE"]),
    ],
    ids=[
        "valid",
        "1-invalid",
    ],
)
def test_add_views_to_local_metastore(
    duckdb_con: duckdb.DuckDBPyConnection, view_dict: dict, view_names: list[str]
) -> None:
    "It builds the views that are passed to it."
    built_view_list = add_views_to_local_metastore(duckdb_con, view_dict)
    assert built_view_list == view_names
