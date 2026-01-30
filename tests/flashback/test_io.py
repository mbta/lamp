import pytest

from lamp_py.flashback.io import get_remote_events, get_vehicle_positions, write_stop_events


# no file --> log error, function ultimately returns empty df
# wrong schema --> log error, function ultimately returns error
# compatible schema --> log nothing, function ultimately returns df
# some records invalid --> log warning, function ultimately returns df with valid records
# non-200 response --> log error, function ultimately returns empty df
def test_get_remote_events():
    """It gracefully handles incomplete or missing remote data."""


# non-200 response --> all records valid, error logged, function ultimately returns
# n failures --> all records valid, error logged, function ultimately raises error
# duplicate ids --> duplicate records filtered out, warning logged, function ultimately returns
def test_get_vehicle_positions():
    """It gracefully handles unexpected responses."""

# use a generator to raise multiple SSL/HTTP exceptions < retry limit --> success
# path doesn't exist --> raise error
# s3 auth error --> raise error
def test_write_stop_events():
    """It gracefully handles failures writing to S3."""
