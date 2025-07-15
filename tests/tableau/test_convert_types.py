import pyarrow
import pytest
from lamp_py.tableau.conversions.convert_types import convert_to_tableau_compatible_schema


# simple case
@pytest.fixture
def test_schema1() -> pyarrow.schema:
    return pyarrow.schema(
        [
            ("service_date", pyarrow.string()),
            ("route_id", pyarrow.large_string()),
            ("exact_plan_trip_match", pyarrow.bool_()),
        ]
    )


# simple case
@pytest.fixture
def test_schema2() -> pyarrow.schema:
    return pyarrow.schema(
        [
            ("service_date", pyarrow.date32()),
            ("route_id", pyarrow.large_string()),
            ("exact_plan_trip_match", pyarrow.bool_()),
            ("start_dt", pyarrow.timestamp(unit="us", tz="UTC")),
        ]
    )


# all case
@pytest.fixture
def test_schema3() -> pyarrow.schema:
    return pyarrow.schema(
        [
            ("service_date", pyarrow.string()),
            ("route_id", pyarrow.large_string()),
            ("exact_plan_trip_match", pyarrow.bool_()),
            ("start_dt", pyarrow.timestamp(unit="us", tz="UTC")),
        ]
    )


def test_tableau_auto_schema_happy_case(test_schema1: pyarrow.schema) -> None:
    assert test_schema1 == convert_to_tableau_compatible_schema(test_schema1)


def test_tableau_auto_schema_override(test_schema1: pyarrow.schema) -> None:
    assert_test_schema1 = pyarrow.schema(
        [
            ("service_date", pyarrow.date32()),  # change to date type
            ("route_id", pyarrow.large_string()),
            ("exact_plan_trip_match", pyarrow.bool_()),
        ]
    )
    overrides = {"service_date": pyarrow.date32()}
    assert assert_test_schema1 == convert_to_tableau_compatible_schema(test_schema1, overrides=overrides)


def test_tableau_auto_schema_exclude(test_schema1: pyarrow.schema) -> None:
    assert_test_schema1 = pyarrow.schema(
        [
            ("exact_plan_trip_match", pyarrow.bool_()),
        ]
    )
    excludes = ["service_date", "route_id"]
    assert assert_test_schema1 == convert_to_tableau_compatible_schema(test_schema1, exclude=excludes)


def test_tableau_auto_schema_tz(test_schema2: pyarrow.schema) -> None:
    assert_test_schema2 = pyarrow.schema(
        [
            ("service_date", pyarrow.date32()),  # change to date type
            ("route_id", pyarrow.large_string()),
            ("exact_plan_trip_match", pyarrow.bool_()),
            ("start_dt", pyarrow.timestamp(unit="us", tz=None)),
        ]
    )
    assert assert_test_schema2 == convert_to_tableau_compatible_schema(test_schema2)


def test_tableau_auto_schema_all_case(test_schema3: pyarrow.schema) -> None:
    breakpoint()
    assert_test_schema3 = pyarrow.schema(
        [
            ("service_date", pyarrow.date32()),  # change to date type
            ("exact_plan_trip_match", pyarrow.bool_()),
            ("start_dt", pyarrow.timestamp(unit="us", tz=None)),
        ]
    )
    overrides = {"service_date": pyarrow.date32()}
    excludes = ["route_id"]
    assert assert_test_schema3 == convert_to_tableau_compatible_schema(
        test_schema3, overrides=overrides, exclude=excludes
    )
