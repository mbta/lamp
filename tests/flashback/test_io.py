# pylint: disable=too-many-positional-arguments,too-many-arguments
from collections.abc import Callable
from contextlib import nullcontext
from logging import ERROR, WARNING
from pathlib import Path
from unittest.mock import AsyncMock, patch

import dataframely as dy
import polars as pl
import pytest
from aiohttp import ClientError
from polars.testing import assert_frame_equal

from lamp_py.flashback.events import StopEvents
from lamp_py.flashback.io import get_remote_events, get_vehicle_positions, write_stop_events
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions
from tests.test_resources import LocalS3Location


@pytest.fixture(name="mock_vp_response")
def fixture_mock_vp_response(tmp_path: Path) -> Callable[[dy.DataFrame[VehiclePositions]], tuple[AsyncMock, bytes]]:
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


@pytest.mark.parametrize(
    "error_type", [FileNotFoundError, OSError, dy.exc.SchemaError], ids=["no-file", "network-error", "schema-error"]
)
def test_get_remote_events(
    dy_gen: dy.random.Generator,
    error_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """It gracefully handles incomplete or missing remote data and networking problems."""
    test_location = LocalS3Location(tmp_path.as_posix(), "test.json")

    StopEvents.sample(2, generator=dy_gen).write_ndjson(test_location.s3_uri)

    if error_type in [OSError, FileNotFoundError]:
        # Simulate networking problems by patching scan_parquet to raise OSError
        with patch("polars.read_ndjson", side_effect=error_type):
            df = get_remote_events(test_location)
        assert WARNING in [record[1] for record in caplog.record_tuples]
    else:
        with patch("dataframely.Schema.filter", side_effect=error_type):
            df = get_remote_events(test_location)
        assert ERROR in [record[1] for record in caplog.record_tuples]

    assert df.is_empty()


@pytest.mark.parametrize(
    ["overrides", "expected_valid_records", "raises_error"],
    [
        (
            {
                "start_date": pl.lit("20231010"),
                "trip_id": pl.lit("5678"),
                "vehicle_id": pl.lit("vehicle_1234"),
                "route_id": pl.lit("red"),
                "stop_sequence": pl.lit(2),
            },
            0,
            True,
        ),
        ({"id": pl.col("id")}, 3, False),
        (
            {
                "start_date": pl.lit("20231010"),
                "trip_id": pl.lit("5678"),
                "vehicle_id": pl.lit("vehicle_1234"),
                "route_id": pl.lit("red"),
                "stop_sequence": pl.Series(values=[2, 2, 3]),
            },
            1,
            True,
        ),
        ({"id": pl.lit(None)}, 0, True),
    ],
    ids=["all-invalid", "all-valid", "1-valid", "wrong-schema"],
)
def test_invalid_remote_events_schema(
    dy_gen: dy.random.Generator,
    overrides: dict[str, pl.Expr],
    expected_valid_records: int,
    raises_error: bool,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """It gracefully handles incomplete or missing remote data."""
    test_location = LocalS3Location(tmp_path.as_posix(), "test.json")

    StopEvents.sample(
        3,
        generator=dy_gen,
    ).with_columns(
        **overrides
    ).write_ndjson(test_location.s3_uri)
    df = get_remote_events(test_location)

    assert df.height >= expected_valid_records
    assert raises_error == (ERROR in [record[1] for record in caplog.record_tuples])


@pytest.mark.parametrize(
    ["num_failures", "max_retries"],
    [
        (1, 10),
        (2, 10),
        (30, 10),
        (5, 3),  # Test with lower max_retries
    ],
    ids=["1-retry", "2-retries", "too-many-retries", "custom-max-retries"],
)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
@patch("lamp_py.flashback.io.sleep")
async def test_get_vehicle_positions(
    mock_sleep: AsyncMock,
    mock_get: AsyncMock,
    dy_gen: dy.random.Generator,
    mock_vp_response: Callable[[dy.DataFrame[VehiclePositions]], tuple[AsyncMock, bytes]],
    num_failures: int,
    max_retries: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It gracefully handles (successive) non-200 responses."""
    vp = VehiclePositions.sample(generator=dy_gen)
    success_response, _ = mock_vp_response(vp)

    # Create mock error response
    error_response = AsyncMock()
    error_response.raise_for_status = lambda: (_ for _ in ()).throw(ClientError("Non-200 response"))

    mock_get.return_value.__aenter__.side_effect = [error_response] * num_failures + [success_response]

    # If too many retries, expect ClientError
    if num_failures >= max_retries:
        with pytest.raises(ClientError):
            await get_vehicle_positions(max_retries=max_retries)
    else:
        df = await get_vehicle_positions(max_retries=max_retries)

        assert df.height == 1
        assert mock_sleep.call_count == num_failures
        # Check that failures were logged (status=failed appears in log message)
        assert "ClientError" in caplog.text
        failure_logs = [record for record in caplog.record_tuples if "status=failed" in record[2]]
        assert len(failure_logs) == num_failures


@pytest.mark.parametrize(
    ["overrides", "expected_rows", "raises_error", "has_invalid_records"],
    [
        (
            {"entity": pl.col("entity").list.eval(pl.element().struct.with_fields(id=pl.lit("a")))},
            0,
            nullcontext(),
            True,
        ),
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
    mock_vp_response: Callable[[dy.DataFrame[VehiclePositions]], tuple[AsyncMock, bytes]],
    overrides: dict[str, pl.Expr],
    expected_rows: int,
    raises_error: pytest.RaisesExc,
    has_invalid_records: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It filters out events that don't comply with the schema."""
    vp = VehiclePositions.sample(generator=dy_gen).with_columns(**overrides)
    mock_response, _ = mock_vp_response(vp)  # type: ignore[arg-type]
    mock_get.return_value.__aenter__.return_value = mock_response

    with raises_error:
        df = await get_vehicle_positions()

        assert df.height == expected_rows
        assert has_invalid_records == (WARNING in [record[1] for record in caplog.record_tuples])


@pytest.mark.parametrize(
    "should_fail",
    [False, True],
    ids=["success", "write-failure"],
)
def test_write_stop_events(
    dy_gen: dy.random.Generator,
    tmp_path: Path,
    should_fail: bool,
) -> None:
    """It writes stop events to S3 and handles write failures."""
    test_location = LocalS3Location(tmp_path.as_posix(), "test.json.gz")
    stop_events = StopEvents.sample(2, generator=dy_gen)

    if should_fail:
        # Simulate persistent write failure
        with patch("polars.DataFrame.write_ndjson", side_effect=OSError("S3 write error")):
            with pytest.raises(OSError):
                write_stop_events(stop_events, test_location)
    else:
        write_stop_events(stop_events, test_location)

        # Verify file was written successfully
        assert Path(test_location.s3_uri).exists()
        written_df = StopEvents.cast(pl.read_ndjson(test_location.s3_uri))
        assert_frame_equal(written_df, stop_events, check_row_order = False)
