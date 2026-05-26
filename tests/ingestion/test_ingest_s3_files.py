from queue import Queue
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from lamp_py.ingestion.ingest_gtfs import ingest_s3_files
from lamp_py.runtime_utils.remote_files import LAMP, S3_INCOMING


class FakePool:
    """Mock Pool class for testing multiprocessing context."""

    def __init__(self) -> None:
        """Initialize pool with empty mapped list."""
        self.mapped: list[object] = []

    def map_async(self, _func: Any, iterable: Any) -> None:
        """Simulate map_async by storing iterable."""
        self.mapped = list(iterable)

    def close(self) -> None:
        """Stub for pool.close()."""
        return None

    def join(self) -> None:
        """Stub for pool.join()."""
        return None

    def __enter__(self) -> "FakePool":
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> bool:
        """Exit context manager."""
        return False


class FakeContext:
    """Mock context for get_context() call."""

    def __init__(self, pool_instance: FakePool) -> None:
        """Initialize with pool."""
        self._pool = pool_instance

    def Pool(self, **_kwargs: Any) -> FakePool:  # noqa: N802
        """Return mock pool."""
        return self._pool


@pytest.mark.parametrize(
    "grouped_files,expected_full_converter_calls,expected_error_files",
    [
        pytest.param(
            {
                "trip-updates": ["lamp/2026-05-01T01:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"],
                "unknown": ["lamp/2026-05-01T01:01:00Z_unknown.json.gz"],
            },
            1,
            ["lamp/2026-05-01T01:01:00Z_unknown.json.gz"],
            id="valid-and-invalid-group",
        ),
        pytest.param({}, 0, [], id="no-files"),
    ],
)
def test_ingest_s3_files(
    grouped_files: dict[str, list[str]],
    expected_full_converter_calls: int,
    expected_error_files: list[str],
) -> None:
    """Test ingest_s3_files correctly routes valid/error files to converters."""
    fake_pool = FakePool()
    full_converter = MagicMock(name="full_converter")
    error_converter = MagicMock(name="error_converter")

    with (
        patch("lamp_py.ingestion.ingest_gtfs.file_list_from_s3", return_value=["ignored.json.gz"]) as mock_file_list,
        patch("lamp_py.ingestion.ingest_gtfs.group_sort_file_list", return_value=grouped_files),
        patch("lamp_py.ingestion.ingest_gtfs.GtfsRtFullPartitionConverter", return_value=full_converter) as mock_full,
        patch("lamp_py.ingestion.ingest_gtfs.NoImplConverter", return_value=error_converter) as mock_error,
        patch("lamp_py.ingestion.ingest_gtfs.get_context", return_value=FakeContext(fake_pool)),
    ):
        ingest_s3_files(metadata_queue=Queue(), bucket_filter=LAMP)

    mock_file_list.assert_called_once_with(bucket_name=S3_INCOMING, file_prefix=LAMP, max_list_size=50000)
    assert mock_full.call_count == expected_full_converter_calls
    mock_error.assert_called_once()
    error_converter.add_files.assert_called_once_with(expected_error_files)

    # one error converter is always scheduled; full converter added when valid files exist
    assert len(fake_pool.mapped) == (1 + expected_full_converter_calls)
