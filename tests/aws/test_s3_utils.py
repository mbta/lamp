# pylint: disable=[W0621, W0611, R0913, R0917]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import boto3
import polars as pl
import pytest
from botocore.stub import ANY, Stubber
from polars.testing import assert_frame_equal
from pyarrow.fs import LocalFileSystem

from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_date_range, move_s3_objects, replace_remote_parquet
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD

from ..test_resources import incoming_dir


@pytest.fixture
def s3_stub():  # type: ignore
    """Test fixture for simulating s3 calls"""
    s3_stub = boto3.client("s3")
    with Stubber(s3_stub) as stubber:
        with patch("lamp_py.aws.s3.get_s3_client", return_value=s3_stub):
            yield stubber
        stubber.assert_no_pending_responses()


@pytest.mark.skip("this wont work in CI without mock...TODO")
def test_file_list_from_s3_date_range() -> None:
    """Test for s3 date range call - query for all files within a range of dates"""
    template = "year={yy}/month={mm}/day={dd}/"
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    s3_uris = file_list_from_s3_date_range(
        bucket_name=S3_SPRINGBOARD,
        file_prefix="LAMP/RT_VEHICLE_POSITIONS/",
        path_template=template,
        end_date=end_date,
        start_date=start_date,
    )

    print(s3_uris)


def test_file_list_s3(s3_stub):  # type: ignore
    """
    Test that list files works as expected given pre-described s3 responses
    """
    # Process 'list_objects_v2' that returns no files
    page_obj_params = {
        "Bucket": "mbta-gtfs-s3",
        "Prefix": ANY,
    }
    page_obj_response = {
        "IsTruncated": False,
        "KeyCount": 0,
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = file_list_from_s3("mbta-gtfs-s3", "")

    assert len(files) == page_obj_response["KeyCount"]
    assert not files

    # Process 'list_objects_v2' that returns dummy 'Contents'
    page_obj_params = {
        "Bucket": "mbta-gtfs-s3",
        "Prefix": ANY,
    }
    page_obj_response = {
        "IsTruncated": False,
        "KeyCount": 2,
        "Contents": [
            {
                "Key": "file1",
                "Size": 1231,
            },
            {
                "Key": "file2",
                "Size": 43212,
            },
        ],
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = file_list_from_s3("mbta-gtfs-s3", "")

    assert len(files) == page_obj_response["KeyCount"]
    should_files = []
    for file_size_dict in page_obj_response["Contents"]:
        name = file_size_dict["Key"]
        should_files.append(f"s3://mbta-gtfs-s3/{name}")

    assert files == should_files

    # Process large page_obj_response from json file
    # 'test_files/large_page_obj_response.json'
    # large json file contains 1,000 Contents records.
    large_response_file = os.path.join(incoming_dir, "large_page_obj_response.json")
    page_obj_params = {
        "Bucket": "mbta-gtfs-s3",
        "Prefix": ANY,
    }
    with open(large_response_file, "r", encoding="utf8") as file:
        page_obj_response = json.load(file)

    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)

    with s3_stub:
        files = file_list_from_s3("mbta-gtfs-s3", "")

    assert len(files) == page_obj_response["KeyCount"]

    should_files = []
    for file_size_dict in page_obj_response["Contents"]:
        name = file_size_dict["Key"]

        should_files.append(f"s3://mbta-gtfs-s3/{name}")

    assert files == should_files


def test_move_bad_objects(s3_stub, caplog):  # type: ignore
    """
    Test that unsuccesful moves are correctly logged
    """
    bad_file_list = [
        "bad_file1",
        "bad_file2",
    ]
    dest_bucket = "bad_dest_bucket"

    caplog.set_level(logging.WARNING)
    with s3_stub:
        move_s3_objects(bad_file_list, dest_bucket)

    log_lines = caplog.text.split("\n")
    found_error = False
    for log_line in log_lines:
        if "ERROR" in log_line:
            print(log_line)
            assert "process_name=move_s3_objects" in caplog.text
            assert "failed_count=2" in caplog.text
            assert "status=failed" in caplog.text
            found_error = True

    assert found_error


@pytest.mark.parametrize(
    ["remote_file_path", "local_records", "upload_succeeds", "log_text", "expected_result"],
    [
        ("remote_foo.parquet", 10, True, "uploaded=True", True),
        ("remote_foo.parquet", 5, True, "LampInvalidReplacementError", False),
        ("remote_bar.parquet", 5, True, "uploaded=True", True),
        ("remote_foo.parquet", 10, False, "uploaded=False", False),
        ("remote_foo.xyz", 10, True, "LampInvalidReplacementError", False),
    ],
    ids=[
        "replace-existing",
        "fewer-records-than-existing",
        "no-existing",
        "upload-fails",
        "existing-not-parquet",
    ],
)
def test_replace_remote_parquet(
    remote_file_path: str,
    local_records: int,
    upload_succeeds: bool,
    log_text: str,
    expected_result: bool,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It replaces a remote parquet or logs an error trying."""
    local_file = tmp_path / "foo.parquet"
    pl.int_range(0, local_records, eager=True).to_frame().write_parquet(local_file.as_posix())

    remote_file = tmp_path / remote_file_path
    pl.int_range(0, 10, eager=True).to_frame().write_parquet(remote_file.as_posix())

    remote_file_exists = "remote_" + local_file.stem == remote_file.stem

    with patch("lamp_py.aws.s3.object_exists", return_value=remote_file_exists):
        with patch("pyarrow.fs.S3FileSystem", return_value=LocalFileSystem()):
            with patch("lamp_py.aws.s3.upload_file", return_value=upload_succeeds):
                result = replace_remote_parquet(local_file.as_posix(), "s3://" + remote_file.as_posix())
                assert_frame_equal(pl.int_range(0, 10, eager=True).to_frame(), pl.read_parquet(remote_file.as_posix()))
                assert log_text in caplog.text
                assert result == expected_result
