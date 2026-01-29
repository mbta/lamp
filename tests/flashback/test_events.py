import dataframely as dy
import polars as pl
import pytest

from lamp_py.flashback.events import (
    StopEventsTable,
    structure_stop_events,
    unnest_vehicle_positions,
)
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions


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
            0,
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
        "null-primary-keys",
        "null-non-primary-keys",
        "empty-entity",
    ],
)
def test_unnest_vehicle_positions(entity: list[dict], valid_records: int) -> None:
    """It gracefully handles missing and complete data alike."""
    vp = VehiclePositions.validate(
        pl.DataFrame([pl.Series(name="entity", values=[entity], dtype=VehiclePositions.entity.dtype)])
    )
    df = unnest_vehicle_positions(vp)
    assert df.height == valid_records


# old records only
# new records only
# mix of old and new records
# 100,000 records
# changing trip
# duplicate records
# multiple stop records
# missing departure
# missing arrival
# non-sequential stop_sequences
def test_update_records():
    """It quickly and correctly updates records."""


def test_structure_stop_events(dy_gen: dy.random.Generator, caplog: pytest.LogCaptureFixture) -> None:
    """It correctly chooses the most recent timestamp and the first trip in the id."""
    events_df = StopEventsTable.sample(num_rows=2, generator=dy_gen, overrides={"id": "foo", "timestamp": [1, 2]})
    events_json = structure_stop_events(events_df)
    assert events_json.row(0)[1] == 2
    assert (
        events_df.with_columns(
            trip=pl.struct("start_date", "trip_id", "direction_id", "route_id", "start_time", "revenue"),
        )
        .select("trip")
        .row(0)[0]
        == events_json.select("trip").row(0)[0]
    )
