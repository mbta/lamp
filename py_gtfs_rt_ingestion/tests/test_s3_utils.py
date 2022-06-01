import pytest
import boto3

from botocore.stub import Stubber
from botocore.stub import ANY
from unittest.mock import patch

from py_gtfs_rt_ingestion.s3_utils import file_list_from_s3

@pytest.fixture(autouse=True)
def s3_stub():
    s3 = boto3.client('s3')
    with Stubber(s3) as stubber:
        with patch("py_gtfs_rt_ingestion.s3_utils.get_s3_client", return_value=s3):
            yield stubber
        stubber.assert_no_pending_responses()

def test_file_list_s3(s3_stub):
    page_obj_params = {
        "Bucket": 'mbta-gtfs-s3',
        "Prefix": ANY,
    }
    page_obj_response = {
        'KeyCount': 2,
        'Contents': [
            {
                'Key':'file1',
                'Size': 1231,
            },
            {
                'Key':'file2',
                'Size': 43212,
            }
        ],
    }

    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params  )
    with s3_stub:
        files = [file for file in file_list_from_s3('mbta-gtfs-s3','')]
        assert files == [(d['Key'], d['Size']) for d in page_obj_response['Contents']]

    page_obj_params = {
        "Bucket": 'mbta-gtfs-s3',
        "Prefix": ANY,
    }
    page_obj_response = {
        'KeyCount': 0,
        'Contents': [],
    }
    s3_stub.add_response("list_objects_v2", page_obj_response, page_obj_params  )
    with s3_stub:
        files = [file for file in file_list_from_s3('mbta-gtfs-s3','')]
        assert files == []