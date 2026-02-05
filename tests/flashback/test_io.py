from contextlib import nullcontext
from logging import WARNING
from pathlib import Path
from unittest.mock import AsyncMock, patch

import dataframely as dy
import polars as pl
import pytest

from lamp_py.flashback.events import StopEventsJSON
from lamp_py.flashback.io import get_remote_events, get_vehicle_positions
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions
from tests.test_resources import LocalS3Location


@pytest.mark.parametrize("file_exists", [False, True], ids=["no-file", "with-file"])
@pytest.mark.parametrize("raise_network_error", [False, True], ids=["no-error", "network-error"])
def test_get_remote_events(
    dy_gen: dy.random.Generator,
    file_exists: bool,
    raise_network_error: bool,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
):
    """It gracefully handles incomplete or missing remote data and networking problems."""
    test_location = LocalS3Location(tmp_path.as_posix(), "test.parquet")

    if file_exists:
        StopEventsJSON.sample(2, generator=dy_gen).write_parquet(test_location.s3_uri)

    if raise_network_error:
        # Simulate networking problems by patching scan_parquet to raise OSError
        with patch("polars.scan_parquet", side_effect=OSError("Network error")):
            df = get_remote_events(test_location)
    else:
        df = get_remote_events(test_location)

    assert (file_exists and not raise_network_error) == (df.height > 0)
    assert (not file_exists or raise_network_error) == (WARNING in [record[1] for record in caplog.record_tuples])


@pytest.mark.parametrize(
    ["overrides", "expected_valid_records", "raise_warning", "raises_error"],
    [
        ({"id": pl.lit("1")}, 0, True, nullcontext()),
        ({"id": pl.col("id")}, 3, False, nullcontext()),
        ({"id": pl.Series(values=["1", "1", "2"])}, 1, True, nullcontext()),
        pytest.param({"id": pl.col("id").implode()}, 0, False, pytest.raises(pl.exceptions.PolarsError)),
    ],
    ids=["all-invalid", "all-valid", "1-valid", "wrong-schema"],
)
def test_invalid_remote_events_schema(
    dy_gen: dy.random.Generator,
    overrides: dict,
    expected_valid_records: int,
    raise_warning: bool,
    raises_error: pytest.RaisesExc,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
):
    """It gracefully handles incomplete or missing remote data."""
    test_location = LocalS3Location(tmp_path.as_posix(), "test.parquet")

    StopEventsJSON.sample(
        3,
        generator=dy_gen,
    ).with_columns(**overrides).write_parquet(test_location.s3_uri)
    with raises_error:
        df = get_remote_events(test_location)

        assert df.height >= expected_valid_records
        assert raise_warning == (WARNING in [record[1] for record in caplog.record_tuples])


# non-200 response --> all records valid, error logged, function ultimately returns
# n failures --> all records valid, error logged, function ultimately raises error
def test_get_vehicle_positions() -> None:
    """It gracefully handles (successive) non-200 responses."""


@pytest.mark.parametrize(
    ["overrides", "expected_rows", "raises_error", "has_invalid_records"],
    [
        ({"entity": pl.col("entity").list.eval(pl.element().struct.with_fields(id=pl.lit("a")))}, 0, nullcontext(), True),
        ({"entity": pl.col("entity")}, 1, nullcontext(), False),  # no change
        (
            {"entity": pl.col("entity").list.eval(pl.element().struct.rename_fields(["id", "trip"]))},
            0,
            nullcontext(),
            True,
        ),  # remove vehicle field
        (
            {"entity": pl.col("entity").list.eval(pl.element().struct.with_fields(vehicle=pl.lit(1)))},
            0,
            pytest.raises(pl.exceptions.SchemaError),
            True,
        ),
    ],
    ids=["duplicate-primary-keys", "valid-data", "null-vehicle", "invalid-schema"],
)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_invalid_vehicle_positions_schema(
    mock_get: AsyncMock,
    dy_gen: dy.random.Generator,
    overrides: dict[str, pl.Expr],
    expected_rows: int,
    raises_error: pytest.RaisesExc,
    has_invalid_records: bool,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """It filters out events that don't comply with the schema."""
    vp = VehiclePositions.sample(generator=dy_gen).with_columns(**overrides)
    json_file = tmp_path.joinpath("test.json")
    vp.write_ndjson(json_file)

    with open(json_file, "rb") as f:
        data = f.read()

    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=data)
    mock_response.raise_for_status = lambda: None  # Non-async method
    mock_get.return_value.__aenter__.return_value = mock_response

    with raises_error:
        df = await get_vehicle_positions()

        assert df.height == expected_rows
        assert has_invalid_records == (WARNING in [record[1] for record in caplog.record_tuples])


# use a generator to raise multiple SSL/HTTP exceptions < retry limit --> success
# path doesn't exist --> raise error
# s3 auth error --> raise error
def test_write_stop_events():
    """It gracefully handles failures writing to S3."""
