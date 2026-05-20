from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from lamp_py.ingestion.daily.trip_updates import within_daily_processing_window


@pytest.mark.parametrize(
    "hour,expected",
    [
        (0, False),
        # Within processing window (1 AM to 7 AM UTC)
        (1, True),
        (2, True),
        (3, True),
        (4, True),
        (5, True),
        (6, True),
        # Outside processing window
        (7, False),
        (8, False),
        (12, False),
        (23, False),
    ],
)
def test_get_daily_processing_window(hour, expected):
    """Test that processing window correctly identifies hours within range."""
    mock_time = datetime(2026, 4, 7, hour, 0, 0, tzinfo=timezone.utc)
    with patch("lamp_py.ingestion.daily.trip_updates.datetime") as mock_datetime:
        mock_datetime.now.return_value = mock_time
        assert within_daily_processing_window() is expected
