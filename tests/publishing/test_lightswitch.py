import os
from contextlib import nullcontext
from logging import WARNING
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
    create_schemas,
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
        assert [(schema_name + ".") if schema_name else "" + v for v in expected_view_names.enter_result] == add_views_to_local_schema(duckdb_con, view_locations, partition_strategy, schema_name)  # type: ignore[attr-defined]


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


def test_register_effective_gtfs_timestamps(
    duckdb_con: duckdb.DuckDBPyConnection,
) -> None:
    """It raises errors if the table doesn't exist or is empty."""
    with duckdb_con:
        register_effective_gtfs_timestamps(duckdb_con, "tests/test_files/SPRINGBOARD/")
        assert duckdb_con.sql("SELECT * FROM gtfs_schedule.effective_timestamps")
        result = duckdb_con.sql(
            "SELECT count(*) FROM gtfs_schedule.effective_timestamps WHERE rating_season IS NULL"
        ).fetchone()
        assert isinstance(result, tuple)
        assert result[0] == 0


def test_register_gtfs_schedule_view(
    duckdb_con: duckdb.DuckDBPyConnection,
) -> None:
    """It raises errors if the table doesn't exist or is empty."""
    with duckdb_con:
        register_effective_gtfs_timestamps(duckdb_con, "tests/test_files/SPRINGBOARD/")
        register_gtfs_schedule_view(duckdb_con, "tests/test_files/SPRINGBOARD/")
        assert duckdb_con.sql("SELECT * FROM gtfs_schedule.date_trip_sequence")


@pytest.mark.parametrize(
    [
        "schema_list",
        "warning_expected",
        "output_schemas",
    ],
    [(["foo", "bar"], False, ["foo", "bar"]), (["foo", "foo"], True, ["foo"]), (["information_schema"], True, [])],
    ids=["unique", "duplicate", "reserved"],
)
def test_create_schemas(
    duckdb_con: duckdb.DuckDBPyConnection,
    schema_list: list[str],
    warning_expected: bool,
    output_schemas: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It raises errors if the schema already exists."""
    resultant_schemas = create_schemas(duckdb_con, schema_list)
    assert (WARNING in [t[1] for t in caplog.record_tuples]) == warning_expected
    assert resultant_schemas == output_schemas
