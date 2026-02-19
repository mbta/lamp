# pylint: disable=too-many-positional-arguments,too-many-arguments
import gzip
import shutil
from datetime import datetime
from json import load
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
    archive_glides_records,
    ingest_glides_events,
)
from lamp_py.runtime_utils.remote_files import S3Location, springboard_glides
from lamp_py.utils.dataframely import extract_pii_columns
from tests.test_resources import LocalS3Location


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
    """It writes PII records to the correct location."""
    converter.records = converter.record_schema.sample(
        num_rows=num_rows,
        generator=dy_gen,
        overrides={
            "time": dy_gen.sample_datetime(
                num_rows, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
            ).cast(pl.Datetime(time_unit="ms"))
        },
    ).to_dicts()

    converter.tmp_dir = tmp_path.as_posix()

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

    pii_columns = [col.name for col in extract_pii_columns(converter.table_schema)]

    if pii_columns:
        assert (
            pq.read_schema(Path(converter.restricted_directory).joinpath(converter.base_filename).as_posix()).names
            == converter.get_table_schema.names
        )
        assert (
            pq.read_metadata(Path(converter.restricted_directory).joinpath(converter.base_filename).as_posix()).num_rows
            == expectation.height + remote_records_height
        )

    assert (
        any(
            col in pq.read_schema(Path(converter.general_directory).joinpath(converter.base_filename).as_posix()).names
            for col in pii_columns
        )
        is False
    )
    assert (
        pq.read_metadata(Path(converter.general_directory).joinpath(converter.base_filename).as_posix()).num_rows
        == expectation.height + remote_records_height
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
def test_ingest_glides_events(
    converter: GlidesConverter,
    dy_gen: dy.random.Generator,
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    events_per_converter: int = 50,
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
    kinesis_reader.stream_name = "test-stream"
    kinesis_reader.get_records.return_value = sample(test_records, len(test_records))

    mock_upload = mocker.Mock(return_value=True)
    mocker.patch("lamp_py.ingestion.glides.upload_file", mock_upload)

    monkeypatch.setattr(GlidesConverter, "tmp_dir", tmp_path.as_posix())

    ingest_glides_events(kinesis_reader, Queue(), upload=True)
    mock_upload.assert_any_call(
        file_name=Path(converter.tmp_dir).joinpath(converter.general_directory, converter.base_filename).as_posix(),
        object_path=springboard_glides.s3_uri + "/" + converter.base_filename,
    )
    if converter.get_pii_column_names:
        mock_upload.assert_any_call(
            file_name=Path(converter.tmp_dir)
            .joinpath(converter.restricted_directory, converter.base_filename)
            .as_posix(),
            object_path=springboard_glides.restricted_s3_uri("GlidesUserEmail") + "/" + converter.base_filename,
        )


@pytest.mark.parametrize(
    ("upload_error", "expected_result"),
    [
        (None, True),
        (OSError("S3 upload failed"), False),
    ],
    ids=["success", "failure"],
)
def test_archive_glides_records(
    tmp_path: Path,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
    upload_error: OSError | None,
    expected_result: bool,
) -> None:
    """It archives records to S3 and logs failures."""
    test_location = LocalS3Location(tmp_path.as_posix(), "archives.json.gz")
    records = [{"time": "2024-01-01T12:00:00Z", "data": {"editorsChanged": {"editors": []}}}]

    def mock_upload(file_name: str, _: S3Location) -> None:
        shutil.copy2(file_name, test_location.s3_uri)
        if upload_error:
            raise upload_error

    mocker.patch("lamp_py.ingestion.glides.upload_file", side_effect=mock_upload)

    result = archive_glides_records(records, test_location, "test-stream")  # type: ignore[arg-type]

    assert result is expected_result

    if expected_result:
        with gzip.open(test_location.s3_uri, "rt", encoding="utf-8") as f:
            assert load(f) == records
    else:
        assert "status=failed" in caplog.text
        assert "S3 upload failed" in caplog.text
