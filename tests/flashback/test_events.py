import time
from datetime import datetime, timedelta

import dataframely as dy
import polars as pl
import pytest

from lamp_py.flashback.events import (
    VehicleEvents,
    VehicleStopEvents,
    aggregate_duration_with_new_records,
    structure_stop_events,
    unnest_vehicle_positions,
)
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositionsApiFormat
from lamp_py.flashback.events import filter_stop_events


@pytest.mark.parametrize(
    [
        "entity",
        "valid_records",
    ],
    [
        (
            [
                {
                    "id": "1234",
                    "vehicle": {
                        "trip": {
                            "trip_id": "5678",
                            "route_id": "red",
                            "start_date": "20231010",
                            "start_time": "08:00:00",
                            "direction_id": 1,
                            "revenue": True,
                            "last_trip": False,
                            "schedule_relationship": "SCHEDULED",
                        },
                        "position": {
                            "latitude": 42.352271,
                            "longitude": -71.055242,
                            "bearing": 90.0,
                        },
                        "vehicle": {
                            "id": "vehicle_1234",
                            "label": "Bus 1234",
                        },
                        "current_stop_sequence": 5,
                        "stop_id": "place-dwnxg",
                        "timestamp": 1700000000,
                        "occupancy_status": "MANY_SEATS_AVAILABLE",
                        "occupancy_percentage": 30,
                        "current_status": "IN_TRANSIT_TO",
                    },
                },
            ],
            1,
        ),
        (
            [
                {
                    "id": "1234",
                    "vehicle": {
                        "trip": {
                            "trip_id": "5678",
                            "route_id": "red",
                        },
                        "position": {},
                        "vehicle": {},
                        "stop_id": "123",
                        "current_stop_sequence": 5,
                        "timestamp": 1700000000,
                    },
                },
            ],
            1,
        ),
        (
            [],
            0,
        ),
    ],
    ids=[
        "complete-data",
        "null-non-primary-keys",
        "empty-entity",
    ],
)
def test_unnest_vehicle_positions(entity: list[dict], valid_records: int) -> None:
    """It gracefully handles missing and complete data alike."""
    vp = VehiclePositionsApiFormat.validate(
        pl.DataFrame([pl.Series(name="entity", values=[entity], dtype=VehiclePositionsApiFormat.entity.dtype)])
    )
    df = unnest_vehicle_positions(vp)
    assert df.height == valid_records

def test_performance_update_records(dy_gen: dy.random.Generator, num_rows: int = 100000) -> None:
    """It can handle 1,000,000 existing and new records in under a second."""

    statuses = ["IN_TRANSIT_TO", "STOPPED_AT", "INCOMING_TO"]

    existing_records = VehicleEvents.sample(
        num_rows=num_rows,
        generator=dy_gen,
        overrides={
            "timestamp": dy_gen.sample_int(
                num_rows, min=int(datetime(1970, 1, 1).timestamp()), max=int(datetime(2039, 1, 1).timestamp())
            ),
            "current_stop_sequence": dy_gen.sample_int(num_rows, min=1, max=50),
            "current_status": dy_gen.sample_choice(num_rows, choices=statuses),
        },
    )
    new_records_count = 1_000
    new_records = VehicleEvents.sample(
        new_records_count,
        generator=dy_gen,
        overrides={
            "id": dy_gen.sample_choice(new_records_count, choices=existing_records.select("id").to_series().to_list()),
            "timestamp": dy_gen.sample_int(
                new_records_count, min=int(datetime(1970, 1, 1).timestamp()), max=int(datetime(2039, 1, 1).timestamp())
            ),
            "current_stop_sequence": dy_gen.sample_int(new_records_count, min=1, max=50),
            "current_status": dy_gen.sample_choice(new_records_count, choices=statuses),
        },
    )

    start = time.time()
    _ = aggregate_duration_with_new_records(existing_records, new_records)
    duration = time.time() - start
    assert duration < 1.0


def test_structure_stop_events(dy_gen: dy.random.Generator) -> None:
    """It correctly chooses the most recent timestamp and the first trip in the id."""
    events_df = VehicleStopEvents.sample(
        num_rows=2, generator=dy_gen, overrides={"id": "foo", "timestamp": [1, 2], "route_id": ["red", "blue"]}
    )
    events_json = structure_stop_events(events_df)
    assert events_json.row(0)[1] == 2
    assert events_df.select("start_date", "trip_id", "direction_id", "route_id", "start_time", "revenue").row(
        0
    ) == events_json.select("start_date", "trip_id", "direction_id", "route_id", "start_time", "revenue").row(0)


@pytest.mark.parametrize(
    [
        "current_status",
        "status_start_timestamp",
        "status_end_timestamp",
        "timestamp",
        "should_pass",
    ],
    [
        ("STOPPED_AT", 2_000_000_000, 2_000_000_000 + 1, int(time.time()), True),
        ("IN_TRANSIT_TO", 2_000_000_000, 2_000_000_000 + 1, int(time.time()), False),
        ("STOPPED_AT", None, None, int(time.time()), False),
        ("STOPPED_AT", 2_000_000_000, None, int(time.time()), True),
        ("STOPPED_AT", None, 2_000_000_000 + 1, int(time.time()), True),
        ("STOPPED_AT", 2_000_000_000, 2_000_000_000 + 1, int(time.time() - 86400 * 8), False),
    ],
    ids=[
        "valid-stopped-at-event",
        "wrong-status",
        "null-timestamps",
        "null-start-only",
        "null-end-only",
        "old-record-outside-max-age",
    ],
)
# pylint: disable=too-many-arguments,too-many-positional-arguments
def test_filter_stop_events(
    dy_gen: dy.random.Generator,
    current_status: str,
    status_start_timestamp: int | None,
    status_end_timestamp: int | None,
    timestamp: int,
    should_pass: bool,
) -> None:
    """It correctly filters stop events by status, timestamps, and age."""

    events = VehicleEvents.sample(
        num_rows=1,
        generator=dy_gen,
        overrides={
            "current_status": current_status,
            "status_start_timestamp": status_start_timestamp,
            "status_end_timestamp": status_end_timestamp,
            "timestamp": timestamp,
        },
    )

    max_record_age = timedelta(days=7)
    filtered = filter_stop_events(events, max_record_age)

    assert (filtered.height == 1) == should_pass
