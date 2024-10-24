from datetime import date
from unittest import mock

from lamp_py.bus_performance_manager.gtfs_utils import (
    bus_routes_for_service_date,
)


@mock.patch("lamp_py.bus_performance_manager.gtfs_utils.object_exists")
def test_bus_routes_for_service_date(exists_patch: mock.MagicMock) -> None:
    """
    Test that bus routes be generated for a given service date. For the
    generated list ensure
        * they don't contain Subway, Commuter Rail, or Ferry routes
        * don't have a leading zero
        * contain a subset of known routes
    """
    exists_patch.return_value = True

    service_date = date(year=2023, month=2, day=1)
    bus_routes = bus_routes_for_service_date(service_date)

    # check that we're getting a non empty list
    assert len(bus_routes) > 0

    subway_routes = [
        "Green-E",
        "Green-B",
        "Green-D",
        "Green-C",
        "Red",
        "Blue",
        "Orange",
    ]

    for route in bus_routes:
        # ensure no commuter rails are being passed through
        assert route[:2] != "CR"

        # ensure no ferries are being passed through
        assert route[:4] != "Boat"

        # ensure no subways are being passed through
        assert route not in subway_routes

        # ensure our routes don't have leading zeros
        assert route[0] != "0"

    known_routes = [
        "741",  # Sliver Line 1
        "34E",  # Walpole Center - Forest Hills Station
        "100",  # Elm Street - Wellington Station
        "504",  # Watertown Yard - Federal Street & Franklin Street
    ]

    for route in known_routes:
        assert route in bus_routes
