from typing import Dict, List, Optional, Tuple, Union

import numpy
import pandas
import pytest

from lamp_py.performance_manager.l0_rt_vehicle_positions import (
    occupancy_from_carriage_details,
    transform_vp_timestamps,
)


Carriage = Dict[str, Union[str, int, None]]
VehiclePosition = Dict[str, Union[str, int, bool, None, List[Carriage], float]]


def carriage(status: Optional[str] = None, percentage: Optional[int] = None) -> Carriage:
    """build a single multi_carriage_details entry"""
    return {"label": "1958", "occupancy_status": status, "occupancy_percentage": percentage}


@pytest.mark.parametrize(
    "details,expected",
    [
        # fully reporting - one entry per carriage, in vehicle_consist order
        (
            [carriage("FEW_SEATS_AVAILABLE", 8), carriage("STANDING_ROOM_ONLY", 21)],
            ("FEW_SEATS_AVAILABLE|STANDING_ROOM_ONLY", "8|21"),
        ),
        # partially reporting - empty slot keeps indexes aligned
        (
            [carriage("FEW_SEATS_AVAILABLE", 8), carriage("NO_DATA_AVAILABLE"), carriage("STANDING_ROOM_ONLY", 15)],
            ("FEW_SEATS_AVAILABLE|NO_DATA_AVAILABLE|STANDING_ROOM_ONLY", "8||15"),
        ),
        # no percentages at all - None, not a string of empty delimiters
        (
            [carriage("NO_DATA_AVAILABLE"), carriage("NO_DATA_AVAILABLE")],
            ("NO_DATA_AVAILABLE|NO_DATA_AVAILABLE", None),
        ),
        ([], (None, None)),
    ],
)
def test_occupancy_from_carriage_details(
    details: List[Carriage],
    expected: Tuple[Optional[str], Optional[str]],
) -> None:
    """multi_carriage_details projects to pipe delimited strings"""
    assert occupancy_from_carriage_details(details) == expected


def vp_record(timestamp: int, is_moving: bool, details: Union[List[Carriage], float]) -> VehiclePosition:
    """build a single vehicle position row for a shared trip-stop"""
    return {
        "service_date": 20260909,
        "route_id": "Orange",
        "trip_id": "trip-1",
        "parent_station": "place-dwnxg",
        "is_moving": is_moving,
        "vehicle_timestamp": timestamp,
        "vehicle_consist": None,
        "multi_carriage_details": details,
    }


def test_transform_vp_timestamps_samples_occupancy_at_arrival() -> None:
    """occupancy comes from the earliest STOPPED_AT, not an in transit record"""
    vehicle_positions = pandas.DataFrame(
        [
            vp_record(1000, True, [carriage("CRUSHED_STANDING_ROOM_ONLY", 95)]),
            # arrival
            vp_record(1100, False, [carriage("STANDING_ROOM_ONLY", 21)]),
            # later, after passengers have alighted
            vp_record(1160, False, [carriage("MANY_SEATS_AVAILABLE", 4)]),
        ]
    )

    result = transform_vp_timestamps(vehicle_positions)

    assert result.shape[0] == 1
    assert result["vp_stop_timestamp"].iloc[0] == 1100
    assert result["occupancy_status"].iloc[0] == "STANDING_ROOM_ONLY"
    assert result["occupancy_percentage"].iloc[0] == "21"


@pytest.mark.parametrize(
    "vehicle_positions",
    [
        # never observed STOPPED_AT, so there is no arrival to sample
        [vp_record(1000, True, [carriage("STANDING_ROOM_ONLY", 21)])],
        # vehicle publishes no multi_carriage_details at all
        [vp_record(1000, True, numpy.nan), vp_record(1100, False, numpy.nan)],
    ],
)
def test_transform_vp_timestamps_without_occupancy(vehicle_positions: List[VehiclePosition]) -> None:
    """occupancy is left NULL when there is nothing to sample"""
    result = transform_vp_timestamps(pandas.DataFrame(vehicle_positions))

    assert result.shape[0] == 1
    assert pandas.isna(result["occupancy_status"].iloc[0])
    assert pandas.isna(result["occupancy_percentage"].iloc[0])
