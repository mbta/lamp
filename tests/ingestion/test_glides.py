from datetime import datetime
from os import remove
from pathlib import Path
from queue import Queue
from random import sample

import dataframely as dy
import polars as pl
import pyarrow.parquet as pq
import pytest
from pytest_mock import MockerFixture

from lamp_py.aws.kinesis import KinesisReader
from lamp_py.ingestion.glides import (
    EditorChanges,
    GlidesConverter,
    OperatorSignIns,
    TripUpdates,
    VehicleTripAssignment,
    ingest_glides_events,
)
from lamp_py.utils.dataframely import has_metadata


@pytest.mark.parametrize(
    [
        "converter",
    ],
    [
        (EditorChanges(),),
        (OperatorSignIns(),),
        (TripUpdates(),),
        (VehicleTripAssignment(),),
    ],
    ids=["editor-changes", "operator-sign-ins", "trip-updates", "vehicle-trip-assignments"],
)
def test_convert_records(dy_gen: dy.random.Generator, converter: GlidesConverter, num_rows: int = 5) -> None:
    """It returns datasets with the expected schema."""
    converter.records = converter.record_schema.sample(
        num_rows=num_rows,
        generator=dy_gen,
        overrides={
            "time": dy_gen.sample_datetime(
                num_rows, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
            ).cast(pl.Datetime(time_unit="ms"))
        },
    ).to_dicts()

    table = converter.convert_records()

    assert not converter.table_schema.validate(table).is_empty()
    assert converter.table_schema.validate(table).select("id").unique().height == num_rows  # all records
    assert set(converter.table_schema.column_names()) == set(table.collect_schema().names())  # no extra columns


@pytest.mark.parametrize(
    ["column_transformations"],
    [
        ({},),
        ({"id": pl.col("id")},),
        ({"new_col": pl.lit(1)},),
        ({"time": pl.col("time").cast(pl.Datetime(time_unit="us")).dt.offset_by("1us")},),
    ],
    ids=["no-remote-records", "same-schema", "dropped-column", "truncated-timestamp"],
)
@pytest.mark.parametrize(
    [
        "converter",
    ],
    [
        (EditorChanges(),),
        (OperatorSignIns(),),
        (TripUpdates(),),
        (VehicleTripAssignment(),),
    ],
    ids=["editor-changes", "operator-sign-ins", "trip-updates", "vehicle-trip-assignments"],
)
def test_append_records(
    dy_gen: dy.random.Generator,
    converter: GlidesConverter,
    tmp_path: Path,
    column_transformations: dict[str, pl.Expr],
    num_rows: int = 5,
) -> None:
    """It writes all records locally using the table schema."""
    converter.records = converter.record_schema.sample(
        num_rows=num_rows,
        generator=dy_gen,
        overrides={
            "time": dy_gen.sample_datetime(
                num_rows, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
            ).cast(pl.Datetime(time_unit="ms"))
        },
    ).to_dicts()

    converter.local_path = tmp_path.joinpath(converter.base_filename).as_posix()

    expectation = converter.convert_records()

    remote_records_height = 0
    if column_transformations:
        remote_records = converter.table_schema.sample(
            num_rows,
            generator=dy_gen,
            overrides={
                "time": dy_gen.sample_datetime(
                    num_rows, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
                ).cast(pl.Datetime(time_unit="ms"))
            },
        ).with_columns(**column_transformations)
        remote_records.write_parquet(converter.local_path)
        remote_records_height = remote_records.height

    converter.append_records()

    pii_columns = [k for k, v in converter.table_schema.columns().items() if has_metadata(v, "reader_roles")]

    assert pq.read_schema(converter.local_path) == converter.get_table_schema
    assert pq.read_metadata(converter.local_path).num_rows == expectation.height + remote_records_height
    assert any(col in pq.read_schema(converter.local_path).names for col in pii_columns) is False


@pytest.mark.parametrize(
    [
        "converter",
    ],
    [
        (EditorChanges(),),
        (OperatorSignIns(),),
        (TripUpdates(),),
        (VehicleTripAssignment(),),
    ],
    ids=["editor-changes", "operator-sign-ins", "trip-updates", "vehicle-trip-assignments"],
)
def test_ingest_glides_events(
    converter: GlidesConverter, dy_gen: dy.random.Generator, mocker: MockerFixture, events_per_converter: int = 50
) -> None:
    """It routes events to correct converter and writes them to specified storage."""
    test_records = (
        converter.record_schema.sample(  # generate test records
            num_rows=events_per_converter,
            generator=dy_gen,
            overrides={
                "time": dy_gen.sample_datetime(
                    events_per_converter, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
                ).cast(pl.Datetime(time_unit="ms"))
            },
        )
        .with_columns(
            time=pl.col("time").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        )  # when records arrive from the kinesis reader they are strings
        .to_dicts()
    )

    kinesis_reader = mocker.Mock(KinesisReader)
    kinesis_reader.get_records.return_value = sample(test_records, len(test_records))

    mock_upload = mocker.Mock(return_value=True)
    mocker.patch("lamp_py.ingestion.glides.upload_file", mock_upload)

    ingest_glides_events(kinesis_reader, Queue(), upload=True)
    assert Path(converter.local_path).exists()
    remove(converter.local_path)  # can't figure out how to patch the tmp_dir inside ingest_glides_events :/
    mock_upload.assert_any_call(file_name=converter.local_path, object_path=converter.remote_path)
