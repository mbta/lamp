# pylint: disable=[W0621, W0611]
# disable these warnings that are triggered by pylint not understanding how test
# fixtures work. https://stackoverflow.com/q/59664605

import json
import os
import pytest

from botocore.stub import ANY

from py_gtfs_rt_ingestion import ConfigType
from py_gtfs_rt_ingestion.batcher import Batch
from py_gtfs_rt_ingestion.batcher import batch_files
from py_gtfs_rt_ingestion.error import ArgumentException
from py_gtfs_rt_ingestion.s3_utils import file_list_from_s3

from .test_s3_utils import s3_stub

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


def test_batch_class(capfd) -> None:  # type: ignore
    """
    test that batch class works as expected
    """
    # Batch object works for each ConfigType
    for each_config in ConfigType:
        batch = Batch(each_config)
        # Checking Batch __str__ method
        print(batch)
        out, _ = capfd.readouterr()
        assert out == f"Batch of 0 bytes in 0 {each_config} files\n"
        assert batch.create_event() == {"files": []}

    # `add_file` method operating correctly
    files = {
        "test100": 100,
        "test200": 200,
    }
    config_type = ConfigType.RT_VEHICLE_POSITIONS
    batch = Batch(config_type=config_type)
    for filename, filesize in files.items():
        batch.add_file(filename=filename, filesize=filesize)
    # Checking Batch __str__ method
    print(batch)
    out, _ = capfd.readouterr()
    assert (
        out == f"Batch of {sum(files.values())} bytes in {len(files)} "
        f"{config_type} files\n"
    )

    # Calling `trigger_lambda` method raises exception
    with pytest.raises(ArgumentException):
        Batch(ConfigType.RT_VEHICLE_POSITIONS).trigger_lambda()


def test_bad_file_names() -> None:
    """
    test that bad filenames do not generate meaningful batches
    """
    files = [("test1", 0), ("test2", 1), ("test3", 0), ("test4", 1)]
    for batch in batch_files(files=files, threshold=100):
        assert batch == []

    # Check mix of good and bad filenames:
    files = [
        ("test1", 0),
        ("test2", 100),
        ("https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz", 100),
    ]
    batches = list(batch_files(files=files, threshold=100))
    assert len(batches) == 1


def test_empty_batch() -> None:
    """
    test that no meaningful batches are generated from an empty file list
    """
    for batch in batch_files(files=[], threshold=100):
        assert batch == []


def test_batch_files(s3_stub) -> None:  # type: ignore
    """
    test that batch files is generating batches as expected for known inputs
    """
    threshold = 100_000

    files = [
        (
            "https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz",
            100_000,
        ),
    ]
    batches = list(batch_files(files=files, threshold=threshold))
    assert len(batches) == len(files)

    files = [
        (
            "https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz",
            threshold,
        ),
        (
            "https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz",
            threshold,
        ),
    ]

    batches = list(batch_files(files=files, threshold=threshold))
    assert len(batches) == len(files)
    for batch in batches:
        assert batch.total_size == threshold

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

    # Verify count of batches produced increases as threshold size decreases
    expected_batches = 0
    thresholds = (100_000, 50_000, 1_000)
    for threshold in thresholds:
        batches = list(batch_files(files=files, threshold=threshold))

        # Verify count of batches in increasing
        assert len(batches) > expected_batches
        expected_batches = len(batches)
        # Verify each batch total_size respects threshold
        # total_size could be greater than threshold if only 1 file in batch
        for batch in batches:
            if len(batch.filenames) > 1:
                assert batch.total_size <= threshold
