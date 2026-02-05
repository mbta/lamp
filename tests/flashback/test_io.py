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


@pytest.fixture
def mock_vp_response(tmp_path: Path):
    """Create mocked vehicle positions HTTP responses from dataframe."""
    def _create(vp: dy.DataFrame[VehiclePositions]) -> tuple[AsyncMock, bytes]:
        json_file = tmp_path.joinpath("test.json")
        vp.write_ndjson(json_file)

        with open(json_file, "rb") as f:
            data = f.read()

        mock_response = AsyncMock()
        mock_response.read = AsyncMock(return_value=data)
        mock_response.raise_for_status = lambda: None

        return mock_response, data

    return _create


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


@pytest.mark.parametrize("num_failures", [1, 2], ids=["single-retry", "multiple-retries"])
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
@patch("lamp_py.flashback.io.sleep")
async def test_get_vehicle_positions(
    mock_sleep: AsyncMock,
    mock_get: AsyncMock,
    dy_gen: dy.random.Generator,
    mock_vp_response,
    num_failures: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It gracefully handles (successive) non-200 responses."""
    from aiohttp import ClientError

    vp = VehiclePositions.sample(generator=dy_gen)
    success_response, _ = mock_vp_response(vp)

    # Create mock responses for failures followed by success
    def mock_raise_for_status_error():
        raise ClientError("Non-200 response")

    error_responses = []
    for _ in range(num_failures):
        error_response = AsyncMock()
        error_response.raise_for_status = mock_raise_for_status_error
        error_responses.append(error_response)

    mock_get.return_value.__aenter__.side_effect = error_responses + [success_response]

    df = await get_vehicle_positions()

    assert df.height == 1
    assert mock_sleep.call_count == num_failures
    # Check that failures were logged (status=failed appears in log message)
    assert "ClientError" in caplog.text
    failure_logs = [record for record in caplog.record_tuples if "status=failed" in record[2]]
    assert len(failure_logs) == num_failures


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
        ),  # remove vehicle field - row exists but with empty entity list (valid data)
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
    mock_vp_response,
    overrides: dict[str, pl.Expr],
    expected_rows: int,
    raises_error: pytest.RaisesExc,
    has_invalid_records: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It filters out events that don't comply with the schema."""
    vp = VehiclePositions.sample(generator=dy_gen).with_columns(**overrides)
    mock_response, _ = mock_vp_response(vp)
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
