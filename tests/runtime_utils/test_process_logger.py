import logging
from pathlib import Path

import pytest
import dataframely as dy
import polars as pl
from polars.testing import assert_frame_equal
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3Location
from lamp_py.aws.ecs import running_in_aws


class Schema(dy.Schema):
    "Trivial schema to test how dataframely reports errors."
    key = dy.Int64(primary_key=True, min=0)
    value1 = dy.Float64(nullable=False)


@pytest.fixture(name="schema")
def fixture_schema() -> type[Schema]:
    "Wrapper around Schema1 for registration as a fixture."
    return Schema


@pytest.fixture(name="patch_bucket")
def fixture_patch_bucket(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    "Replace error URI with local temp directory, returning the temp directory for access."
    if running_in_aws():
        monkeypatch.setattr(S3Location, "s3_uri", tmp_path.as_uri())
    else:
        monkeypatch.setenv("TEMP_DIR", tmp_path.as_posix())

    return tmp_path


def test_unstarted_log(caplog: pytest.LogCaptureFixture) -> None:
    "It logs unraised validation errors with the correct type and message."

    process_logger = ProcessLogger("test_unstarted_log")
    process_logger.add_metadata(foo="bar")
    process_logger.log_failure(Exception("test"))
    process_logger.log_complete()

    assert "status=complete" in caplog.text


def test_unraised_exception(caplog: pytest.LogCaptureFixture) -> None:
    "It doesn't output `NoneType: None` when the exception has no traceback."

    process_logger = ProcessLogger("test_not_none")
    process_logger.log_start()

    exception = Exception("foo")

    process_logger.log_failure(Exception(exception))

    assert not exception.__traceback__
    assert "NoneType: None" not in caplog.text.splitlines()


def test_start_logging_explicitly(caplog: pytest.LogCaptureFixture) -> None:
    "It doesn't start the log when it initializes."

    ProcessLogger("test_not_none", foo="bar")

    assert caplog.text == ""


def test_2_errors(schema: type[Schema], caplog: pytest.LogCaptureFixture, patch_bucket: Path) -> None:
    "It gracefully logs 2 errors as warnings."
    test_bucket = patch_bucket
    process_logger = ProcessLogger("test_2_errors")

    df = pl.DataFrame({"key": range(-1, 9), "value1": [float(n) for n in range(0, 9)] + [None]})

    _ = process_logger.log_dataframely_filter_results(*schema().filter(df))

    assert "ValidationError: error_type=key|min" in caplog.text
    assert "ValidationError: error_type=value1|nullability" in caplog.text
    assert "invalid_records=2\n" in caplog.text
    assert logging.WARNING in [r[1] for r in caplog.record_tuples]
    assert pl.read_parquet(test_bucket).height == 2


def test_1_error(
    schema: type[Schema], caplog: pytest.LogCaptureFixture, dy_gen: dy.random.Generator, patch_bucket: Path
) -> None:
    "It gracefully logs 1 error as a warning."
    test_bucket = patch_bucket
    process_logger = ProcessLogger("test_1_error")

    df = pl.DataFrame({"key": range(-1, 9), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    _ = process_logger.log_dataframely_filter_results(*schema().filter(df))

    assert "ValidationError: error_type=key|min" in caplog.text
    assert "invalid_records=1\n" in caplog.text
    assert logging.WARNING in [r[1] for r in caplog.record_tuples]
    assert pl.read_parquet(test_bucket).height == 1


def test_error_logging_level(
    schema: type[Schema], caplog: pytest.LogCaptureFixture, dy_gen: dy.random.Generator, patch_bucket: Path
) -> None:
    "It gracefully logs 1 error as an error."
    _ = patch_bucket
    process_logger = ProcessLogger("test_1_error")

    df = pl.DataFrame({"key": range(-1, 9), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    _ = process_logger.log_dataframely_filter_results(*schema().filter(df), logging.ERROR)

    assert "ValidationError: error_type=key|min" in caplog.text
    assert "invalid_records=1\n" in caplog.text
    assert logging.ERROR in [r[1] for r in caplog.record_tuples]


def test_0_errors(
    schema: type[Schema],
    caplog: pytest.LogCaptureFixture,
    dy_gen: dy.random.Generator,
) -> None:
    "It logs 0 validation_errors and returns the entire dataframe."
    process_logger = ProcessLogger("test_no_errors")

    df1 = pl.DataFrame({"key": range(0, 10), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    valid = process_logger.log_dataframely_filter_results(*schema().filter(df1))

    assert "ValidationError" not in caplog.text
    assert "invalid_records=0\n" in caplog.text

    assert_frame_equal(df1, valid)
