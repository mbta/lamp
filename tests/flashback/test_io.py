import pytest

from lamp_py.flashback.io import get_remote_events, get_vehicle_positions


# no file
# wrong schema
# compatible schema
def test_get_remote_events():
    """It gracefully handles incomplete or missing remote data."""


# ssl error
# non-200 response
# n failures
# incompatible file
def test_get_vehicle_positions():
    """It gracefully handles unexpected responses."""
