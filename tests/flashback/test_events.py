import pytest
from lamp_py.flashback.events import (
    StopEventsJSON,
    StopEventsTable,
    structure_stop_events,
    unnest_vehicle_positions,
    update_records,
)


# missing columns --> error
# incorrect data --> warning
# correct data --> pass
def test_unnest_vehicle_positions():
    """It gracefully handles missing and complete data alike."""


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


# changing route
def test_structure_stop_events():
    """It correctly structures flat stop events into nested format."""
