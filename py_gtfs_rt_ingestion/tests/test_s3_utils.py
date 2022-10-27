# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import logging
import os

from unittest.mock import patch

import boto3
import pytest

from botocore.stub import Stubber
from botocore.stub import ANY

from lib.s3_utils import file_list_from_s3
from lib.s3_utils import move_s3_objects

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


@pytest.fixture
def s3_stub():  # type: ignore
    """test fixture for simulating s3 calls"""
    s3_stub = boto3.client("s3")
    with Stubber(s3_stub) as stubber:
        with patch("lib.s3_utils.get_s3_client", return_value=s3_stub):
            yield stubber
        stubber.assert_no_pending_responses()


def test_file_list_s3(s3_stub):  # type: ignore
    """
    test that list files works as expected given pre-described s3 responses
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
        files = list(file_list_from_s3("mbta-gtfs-s3", ""))

    assert len(files) == page_obj_response["KeyCount"]
    should_files = []
    for file_size_dict in page_obj_response["Contents"]:
        name = file_size_dict["Key"]
        should_files.append(f"s3://mbta-gtfs-s3/{name}")

    assert files == should_files

    # Process large page_obj_response from json file
    # 'test_files/large_page_obj_response.json'
    # large json file contains 1,000 Contents records.
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

        should_files.append(f"s3://mbta-gtfs-s3/{name}")

    assert files == should_files


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

    assert "failed=move_s3_object" in caplog.text

    for bad_file in bad_file_list:
        assert f"filename={bad_file}" in caplog.text
        assert f"destination={dest_bucket}" in caplog.text
