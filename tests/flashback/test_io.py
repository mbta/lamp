from contextlib import nullcontext
from logging import WARNING
from pathlib import Path
from unittest.mock import AsyncMock, patch

import dataframely as dy
import polars as pl
import pytest

from lamp_py.flashback.io import get_vehicle_positions
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions


# no file --> log error, function ultimately returns empty df
# wrong schema --> log error, function ultimately returns error
# compatible schema --> log nothing, function ultimately returns df
# some records invalid --> log warning, function ultimately returns df with valid records
# non-200 response --> log error, function ultimately returns empty df
def test_get_remote_events():
    """It gracefully handles incomplete or missing remote data."""


# non-200 response --> all records valid, error logged, function ultimately returns
# n failures --> all records valid, error logged, function ultimately raises error


@pytest.mark.parametrize(
    ["overrides", "valid_records", "has_invalid_records"],
    [
        ({"entity": pl.col("entity").list.eval(pl.element().struct.with_fields(id=pl.lit("a")))}, nullcontext(0), True),
        ({"entity": pl.col("entity")}, nullcontext(1), False),  # no change
        (
            {"entity": pl.col("entity").list.eval(pl.element().struct.rename_fields(["id", "trip"]))},
            nullcontext(0),
            True,
        ),  # remove vehicle field
        (
            {"entity": pl.col("entity").list.eval(pl.element().struct.with_fields(vehicle=pl.lit(1)))},
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
    valid_records: pytest.RaisesExc,
    has_invalid_records: bool,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """It filters out events that don't comply with the schema."""
    vp = VehiclePositions.sample(generator=dy_gen).with_columns(**overrides)
    vp.write_ndjson(tmp_path.joinpath("test.json"))
    mock_get.return_value.__aenter__.return_value.read = AsyncMock(side_effect=[tmp_path.joinpath("test.json")])
    with valid_records:
        df = await get_vehicle_positions()

        assert df.height == valid_records.enter_result  # ty: ignore[unresolved-attribute]
        assert has_invalid_records == (WARNING in [record[1] for record in caplog.record_tuples])


# use a generator to raise multiple SSL/HTTP exceptions < retry limit --> success
# path doesn't exist --> raise error
# s3 auth error --> raise error
def test_write_stop_events():
    """It gracefully handles failures writing to S3."""
