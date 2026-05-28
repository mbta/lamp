from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from lamp_py.ingestion.daily.trip_updates import within_daily_processing_window


@pytest.mark.parametrize(
    "hour,expected",
    [
        # Outside processing window
        (0, False),  # midnight
        (23, False),  # 11 PM UTC
        # Within processing window (1 AM to 11 PM UTC, hour < 23)
        (1, True),
        (2, True),
        (7, True),
        (8, True),
        (12, True),
        (22, True),
    ],
)
def test_get_daily_processing_window(hour: int, expected: bool) -> None:
    """Test that processing window correctly identifies hours within range [1, 23)."""
    mock_time = datetime(2026, 4, 7, hour, 0, 0, tzinfo=timezone.utc)
    with patch("lamp_py.ingestion.daily.trip_updates.datetime") as mock_dt:
        mock_dt.now.return_value = mock_time
        assert within_daily_processing_window() is expected
