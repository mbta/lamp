import boto3
import json
import os
import pytest

from botocore.stub import Stubber
from botocore.stub import ANY
from unittest.mock import patch

from py_gtfs_rt_ingestion.s3_utils import file_list_from_s3

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")

@pytest.fixture
def s3_stub():
    s3 = boto3.client('s3')
    with Stubber(s3) as stubber:
        with patch("py_gtfs_rt_ingestion.s3_utils.get_s3_client", return_value=s3):
            yield stubber
        stubber.assert_no_pending_responses()

def test_file_list_s3(s3_stub):
    # Process 'list_objects_v2' that returns no files
    page_obj_params = {
        "Bucket": 'mbta-gtfs-s3',
        "Prefix": ANY,
    }
    page_obj_response = {
        'IsTruncated': False,
        'KeyCount': 0,
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = [file for file in file_list_from_s3('mbta-gtfs-s3','')]

    assert len(files) == page_obj_response['KeyCount']
    assert files == []

    # Process 'list_objects_v2' that returns dummy 'Contents'
    page_obj_params = {
        "Bucket": 'mbta-gtfs-s3',
        "Prefix": ANY,
    }
    page_obj_response = {
        'IsTruncated': False,
        'KeyCount': 2,
        'Contents': [
            {
                'Key': 'file1',
                'Size': 1231,
            },
            {
                'Key': 'file2',
                'Size': 43212,
            }
        ],
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = [file for file in file_list_from_s3('mbta-gtfs-s3','')]

    assert len(files) == page_obj_response['KeyCount']
    assert files == [(d['Key'], d['Size']) for d in page_obj_response['Contents']]

    # Process large page_obj_response from json file 'test_files/large_page_obj_response.json'
    # large json file contains 1,000 Contents records.
    large_response_file = os.path.join(TEST_FILE_DIR,"large_page_obj_response.json")
    page_obj_params = {
        "Bucket": 'mbta-gtfs-s3',
        "Prefix": ANY,
    }
    with open(large_response_file, 'r') as f:
        page_obj_response = json.load(f)
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params)
    with s3_stub:
        files = [file for file in file_list_from_s3('mbta-gtfs-s3','')]

    assert len(files) == page_obj_response['KeyCount']
    assert files == [(d['Key'], d['Size']) for d in page_obj_response['Contents']]

