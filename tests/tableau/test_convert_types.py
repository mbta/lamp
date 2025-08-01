import os
import pyarrow
import pytest
from lamp_py.tableau.conversions.convert_types import (
    convert_to_tableau_compatible_schema,
    get_default_tableau_schema_from_s3,
)

from lamp_py.runtime_utils.remote_files import S3Location, bus_events

import polars as pl

# from ..test_resources import rt_vehicle_positions as s3_vp


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
    assert assert_test_schema1 == convert_to_tableau_compatible_schema(test_schema1, excludes=excludes)


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
        test_schema3, overrides=overrides, excludes=excludes
    )


def test_schema_default_from_s3() -> None:
    bus_events = S3Location(bucket="mbta-performance", prefix=os.path.join("lamp", "bus_vehicle_events"), version="1.2")
    overrides = {"service_date": pyarrow.date32()}
    input_schema, output_schema = get_default_tableau_schema_from_s3(input_location=bus_events, overrides=overrides)
    # print(input_schema)`
    # print(output_schema)`
    print(list(set(input_schema).difference(output_schema)))
    # res = [pyarrow.Field('stop_arrival_dt', timestamp(us, tz=UTC)), pyarrow.Field<gtfs_sort_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_arrival_dt: timestamp[us, tz=UTC]>, pyarrow.Field<stop_departure_dt: timestamp[us, tz=UTC]>, pyarrow.Field<service_date: large_string>, pyarrow.Field<tm_actual_arrival_dt: timestamp[us, tz=UTC]>, pyarrow.Field<tm_scheduled_time_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_travel_to_dt: timestamp[us, tz=UTC]>, pyarrow.Field<tm_actual_departure_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_departure_dt: timestamp[us, tz=UTC]>]
    # diff = {pyarrow.Field<tm_actual_arrival_dt: timestamp[us, tz=UTC]>, pyarrow.Field<service_date: large_string>, pyarrow.Field<tm_scheduled_time_dt: timestamp[us, tz=UTC]>, pyarrow.Field<stop_arrival_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_arrival_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_departure_dt: timestamp[us, tz=UTC]>, pyarrow.Field<stop_departure_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_travel_to_dt: timestamp[us, tz=UTC]>, pyarrow.Field<tm_actual_departure_dt: timestamp[us, tz=UTC]>, pyarrow.Field<gtfs_sort_dt: timestamp[us, tz=UTC]>}
    breakpoint()


def test_spare_schema():
    df = pl.read_parquet("s3://mbta-ctd-dataplatform-staging-springboard/spare/vehicles.parquet")

    convert_to_tableau_compatible_schema(df.to_arrow().schema)
