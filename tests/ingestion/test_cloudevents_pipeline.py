"""Tests for CloudEvent ingestion batching."""

from datetime import datetime, timezone

from lamp_py.ingestion_cloudevents.pipeline import select_files


def test_select_files_adds_each_file_once_and_respects_byte_limit() -> None:
    """Select oldest fitting files without repeatedly adding the same object."""
    files = [
        {"s3_obj_path": "s3://bucket/new", "size_bytes": 4, "last_modified": datetime(2025, 1, 3, tzinfo=timezone.utc)},
        {"s3_obj_path": "s3://bucket/old", "size_bytes": 6, "last_modified": datetime(2025, 1, 1, tzinfo=timezone.utc)},
        {"s3_obj_path": "s3://bucket/mid", "size_bytes": 5, "last_modified": datetime(2025, 1, 2, tzinfo=timezone.utc)},
    ]

    assert select_files(files, max_batch_bytes=10) == ["s3://bucket/old", "s3://bucket/new"]
    assert files[0]["s3_obj_path"] == "s3://bucket/new"  # Do not mutate the input list.


def test_select_files_handles_empty_and_oversized_files() -> None:
    """Never select an object larger than the byte budget."""
    files = [
        {"s3_obj_path": "s3://bucket/large", "size_bytes": 11, "last_modified": datetime(2025, 1, 1)},
        {"s3_obj_path": "s3://bucket/small", "size_bytes": 2, "last_modified": datetime(2025, 1, 2)},
    ]

    assert select_files([], max_batch_bytes=10) == []
    assert select_files(files, max_batch_bytes=10) == ["s3://bucket/small"]
