# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import logging
import os

from datetime import datetime, timezone

from unittest.mock import patch

import boto3
import pytest

from botocore.stub import Stubber
from botocore.stub import ANY

from py_gtfs_rt_ingestion.s3_utils import file_list_from_s3
from py_gtfs_rt_ingestion.s3_utils import move_s3_objects

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


@pytest.fixture
def s3_stub():  # type: ignore
    """test fixture for simulating s3 calls"""
    s3_stub = boto3.client("s3")
    with Stubber(s3_stub) as stubber:
        with patch(
            "py_gtfs_rt_ingestion.s3_utils.get_s3_client", return_value=s3_stub
        ):
            yield stubber
        stubber.assert_no_pending_responses()


def test_file_list_s3_empty(s3_stub):  # type: ignore
    """
    test that list files works as expected given empty s3 responses
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
        files = list(file_list_from_s3("mbta-gtfs-s3", ""))

    assert len(files) == page_obj_response["KeyCount"]
    assert not files


def test_file_list_s3_contents(s3_stub):  # type: ignore
    """
    Process 'list_objects_v2' that returns dummy 'Contents'
    """
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
                "LastModified": "2019-02-08 00:00:10+00:00",
            },
            {
                "Key": "file2",
                "Size": 43212,
                "LastModified": "2019-02-08 00:00:10+00:00",
            },
        ],
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = list(file_list_from_s3("mbta-gtfs-s3", ""))

    assert len(files) == page_obj_response["KeyCount"]
    should_files = []
    for file_size_dict in page_obj_response["Contents"]:
        name = file_size_dict["Key"]
        size = file_size_dict["Size"]

        should_files.append((f"s3://mbta-gtfs-s3/{name}", size))

    assert files == should_files


def test_file_list_s3_large(s3_stub):  # type: ignore
    """
    check that large responses from s3 work for file_list_from_s3 function
    """
    large_response_file = os.path.join(
        TEST_FILE_DIR, "large_page_obj_response.json"
    )
    page_obj_params = {
        "Bucket": "mbta-gtfs-s3",
        "Prefix": ANY,
    }
    with open(large_response_file, "r", encoding="utf8") as file:
        page_obj_response = json.load(file)

    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)

    with s3_stub:
        files = list(file_list_from_s3("mbta-gtfs-s3", ""))

    assert len(files) == page_obj_response["KeyCount"]

    should_files = []
    for file_size_dict in page_obj_response["Contents"]:
        name = file_size_dict["Key"]
        size = file_size_dict["Size"]

        should_files.append((f"s3://mbta-gtfs-s3/{name}", size))

    assert files == should_files


def test_file_list_s3_until(s3_stub):  # type: ignore
    """
    Test that time limiting works for file_list_from_s3 function
    """
    large_response_file = os.path.join(
        TEST_FILE_DIR, "large_page_obj_response.json"
    )
    page_obj_params = {
        "Bucket": "mbta-gtfs-s3",
        "Prefix": ANY,
    }
    with open(large_response_file, "r", encoding="utf8") as file:
        page_obj_response = json.load(file)

    # update the last modified field, changing from iso time format to datetime
    # object like expected from aws.
    for entry in page_obj_response["Contents"]:
        entry["LastModified"] = datetime.fromisoformat(entry["LastModified"])

    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)

    until = datetime(
        year=2019, month=2, day=8, hour=1, minute=0, tzinfo=timezone.utc
    )

    with s3_stub:
        files = list(file_list_from_s3("mbta-gtfs-s3", "", until))

    # some of the files will be omitted because they are filterd out by date
    assert len(files) < page_obj_response["KeyCount"]

    # for the files that pass, pull the timestamps out of their filename and
    # check that their hour is zero as all files with times after hour 0 will be
    # filterd out.
    for filename, _ in files:
        # strip off all prefixes
        filename = filename.split("/")[-1]

        # iso date string is everything in filename before first _
        date_string = filename.split("_")[0]

        # convert to datetime and assert hour is 0
        assert datetime.fromisoformat(date_string).hour == 0


def test_move_bad_objects(s3_stub, caplog):  # type: ignore
    """
    test that unsuccesful moves are correctly logged
    """
    bad_file_list = [
        "bad_file1",
        "bad_file2",
    ]
    dest_bucket = "bad_dest_bucket"

    caplog.set_level(logging.WARNING)
    with s3_stub:
        move_s3_objects(bad_file_list, dest_bucket)

    for bad_file in bad_file_list:
        assert f"Unable to move {bad_file} to {dest_bucket}" in caplog.text
