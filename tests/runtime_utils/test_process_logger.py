import logging
import pytest

import dataframely as dy
import polars as pl
from polars.testing import assert_frame_equal
from lamp_py.runtime_utils.process_logger import ProcessLogger


class Schema1(dy.Schema):  # pylint: disable=missing-class-docstring
    key = dy.Int64(primary_key=True, min=0)
    value1 = dy.Float64(nullable=False)


class Schema2(dy.Schema):  # pylint: disable=missing-class-docstring
    key = dy.Int64(primary_key=True)
    value2 = dy.String(nullable=True, regex=r"[a-z]+")


class FakeCollection(dy.Collection):  # pylint: disable=missing-class-docstring
    first: dy.LazyFrame[Schema1]
    second: dy.LazyFrame[Schema2]

    @dy.filter()
    def all_keys_match(self) -> pl.LazyFrame:
        "Each key in the first dataframe is in each key in the second dataframe and vice-versa."
        return self.first.join(self.second, on="key", how="inner")


@pytest.fixture(name="schema_1")
def fixture_schema_1() -> type[Schema1]:
    "Wrapper around Schema1 for registration as a fixture."
    return Schema1


@pytest.fixture(name="schema_2")
def fixture_schema_2() -> type[Schema2]:
    "Wrapper around Schema2 for registration as a fixture."
    return Schema2


@pytest.fixture(name="fake_collection")
def fixture_fake_collection() -> type[FakeCollection]:
    "Wrapper around FakeCollection for registration as a fixture."
    return FakeCollection


@pytest.fixture(name="dy_gen", params=[1])
def fixture_dataframely_random_generator(request: pytest.FixtureRequest) -> dy.random.Generator:
    "Fixture wrapper around dataframely random data generator."
    return dy.random.Generator(request.param)


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


def test_one_filtered_schema(
    schema_1: type[Schema1], caplog: pytest.LogCaptureFixture, dy_gen: dy.random.Generator
) -> None:
    "It gracefully logs one filtered schema."

    process_logger = ProcessLogger("test_one_filtered_schema")

    df = pl.DataFrame({"key": range(-1, 9), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    _ = process_logger.log_dataframely_filter_results(schema_1().filter(df))

    assert "dataframely.exc.ValidationError: key|min" in caplog.text
    assert "validation_errors=1\n" in caplog.text


def test_multiple_filtered_schemata(
    schema_1: type[Schema1], schema_2: type[Schema2], caplog: pytest.LogCaptureFixture, dy_gen: dy.random.Generator
) -> None:
    "It gracefully handles multiple schemas and returns only the valid results from the first schema."
    process_logger = ProcessLogger("test_multiple_filtered_schemata")

    df1 = pl.DataFrame({"key": range(-1, 9), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    df2 = pl.DataFrame({"key": range(-1, 9), "value2": dy_gen.sample_string(10, regex=r"[0-9]+")})

    _ = process_logger.log_dataframely_filter_results(schema_1().filter(df1), schema_2().filter(df2))

    with caplog.at_level(logging.ERROR):
        assert "dataframely.exc.ValidationError: key|min, value2|regex" in caplog.text

    with caplog.at_level(logging.INFO):
        assert "validation_errors=11\n" in caplog.text


def test_one_schema_one_collection(
    schema_1: type[Schema1],
    fake_collection: type[FakeCollection],
    caplog: pytest.LogCaptureFixture,
    dy_gen: dy.random.Generator,
) -> None:
    "It gracefully handles multiple schemas and returns only the valid results from the first schema."
    process_logger = ProcessLogger("test_one_schema_one_collection")

    df1 = pl.DataFrame({"key": range(-1, 9), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    df2 = pl.DataFrame({"key": range(9, 19), "value2": dy_gen.sample_string(10, regex=r"[0-9]+")})

    _ = process_logger.log_dataframely_filter_results(
        schema_1().filter(df1), fake_collection().filter({"first": df1, "second": df2})
    )

    with caplog.at_level(logging.ERROR):
        assert all(e in caplog.text for e in ["dataframely.exc.ValidationError", "all_keys_match", "value2|regex", "key|min"])

    with caplog.at_level(logging.INFO):
        assert "validation_errors=21\n" in caplog.text


def test_no_errors(
    schema_1: type[Schema1],
    fake_collection: type[FakeCollection],
    caplog: pytest.LogCaptureFixture,
    dy_gen: dy.random.Generator,
) -> None:
    "It logs 0 validation_errors and returns the entire dataframe."
    process_logger = ProcessLogger("test_no_errors")

    df1 = pl.DataFrame({"key": range(0, 10), "value1": dy_gen.sample_float(10, min=0, max=10000)})

    df2 = pl.DataFrame({"key": range(0, 10), "value2": dy_gen.sample_string(10, regex=r"[a-z]+")})

    valid = process_logger.log_dataframely_filter_results(
        schema_1().filter(df1), fake_collection().filter({"first": df1, "second": df2})
    )

    assert logging.ERROR not in [r[1] for r in caplog.record_tuples]

    assert "validation_errors=0\n" in caplog.text

    assert_frame_equal(df1, valid)
