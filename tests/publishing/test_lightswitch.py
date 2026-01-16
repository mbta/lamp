import os
from contextlib import nullcontext
from logging import ERROR
from pathlib import Path
from typing import Generator

import polars as pl
import pytest
import duckdb

from lamp_py.publishing.lightswitch import (
    build_view,
    add_views_to_local_schema,
    register_read_ymd,
    register_effective_gtfs_timestamps,
    register_gtfs_schedule_view,
    register_gtfs_service_id_table,
    create_schemas_if_not_exists,
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
    duckdb_con: duckdb.DuckDBPyConnection,
    partition_strategy: str,
    data_location: S3Location,
    result: pytest.RaisesExc,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It gracefully creates the view using the specified partition strategy."""
    assert result == build_view(duckdb_con, "test", data_location, partition_strategy)
    assert "view_name=test" in caplog.text


@pytest.mark.parametrize(["schema_name"], [(None,), ("foo",)], ids=["no-schema", "has-schema"])
@pytest.mark.parametrize(
    ["view_locations", "partition_strategy", "expected_view_names"],
    [
        ([rt_vehicle_positions], "/year=*/month=*/day=*/hour=*/*.parquet", nullcontext(["RT_VEHICLE_POSITIONS"])),
        (
            [rt_vehicle_positions, tm_route_file],
            "/year=*/month=*/day=*/hour=*/*.parquet",
            nullcontext(["RT_VEHICLE_POSITIONS"]),
        ),
        ([rt_vehicle_positions, tm_route_file], "fake strategy", pytest.raises(Exception)),
    ],
    ids=[
        "valid",
        "1-valid",
        "all-invalid",
    ],
)
def test_add_views_to_local_schema(
    duckdb_con: duckdb.DuckDBPyConnection,
    view_locations: list[S3Location],
    partition_strategy: str,
    schema_name: str | None,
    expected_view_names: pytest.RaisesExc,
) -> None:
    """It builds the views that are passed to it."""
    with expected_view_names:
        assert [((schema_name + ".") if schema_name is not None else "") + v for v in expected_view_names.enter_result] == add_views_to_local_schema(duckdb_con, view_locations, partition_strategy, schema_name)  # type: ignore[attr-defined]


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


@pytest.fixture(name="effective_gtfs_timestamps_table")
def fixture_effective_gtfs_timestamps(
    duckdb_con: duckdb.DuckDBPyConnection,
) -> bool:
    """Create effective timestamp table."""
    return register_effective_gtfs_timestamps(duckdb_con, "tests/test_files/SPRINGBOARD/")


@pytest.fixture(name="gtfs_service_id_table")
def fixture_gtfs_service_id_table(
    duckdb_con: duckdb.DuckDBPyConnection,
    effective_gtfs_timestamps_table: bool,
) -> bool:
    """Create service_ids table."""
    return effective_gtfs_timestamps_table & register_gtfs_service_id_table(duckdb_con, "tests/test_files/SPRINGBOARD/")


def test_register_gtfs_schedule_view(
    duckdb_con: duckdb.DuckDBPyConnection,
    gtfs_service_id_table: bool,
) -> None:
    """It raises errors if the table doesn't exist or is empty."""
    if gtfs_service_id_table:
        register_gtfs_schedule_view(duckdb_con, "tests/test_files/SPRINGBOARD/")
    assert duckdb_con.sql("SELECT * FROM gtfs_schedule.date_trip_sequence")


@pytest.mark.parametrize(
    [
        "schema_list",
        "error_expected",
        "output_schemas",
    ],
    [(["foo", "bar"], False, ["foo", "bar"]), (["foo", "foo"], False, ["foo", "foo"]), ([""], True, [])],
    ids=["unique", "duplicate", "empty"],
)
def test_create_schemas_if_not_exists(
    duckdb_con: duckdb.DuckDBPyConnection,
    schema_list: list[str],
    error_expected: bool,
    output_schemas: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It raises errors if the schema already exists."""
    resultant_schemas = create_schemas_if_not_exists(duckdb_con, schema_list)
    assert (ERROR in [t[1] for t in caplog.record_tuples]) == error_expected
    assert resultant_schemas == output_schemas
