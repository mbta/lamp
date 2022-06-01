import pytest

from py_gtfs_rt_ingestion import ConfigType

from py_gtfs_rt_ingestion.batcher import Batch
from py_gtfs_rt_ingestion.batcher import batch_files
from py_gtfs_rt_ingestion.error import NoImplException

def test_batch_class(capfd):
    for each_config in ConfigType:
        batch = Batch(each_config)
        # Checking Batch __str__ method
        print(batch)
        out, err = capfd.readouterr()
        assert out == f"Batch of 0 bytes in 0 {each_config} files\n" 
        assert batch.create_event() == {'files':[]}

    with pytest.raises(NoImplException):
        Batch(ConfigType.RT_VEHICLE_POSITIONS).trigger_lambda()

    files = {
        'test100': 100,
        'test200': 200,
    }
    config_type = ConfigType.RT_VEHICLE_POSITIONS
    batch = Batch(config_type=config_type)
    for filename, filesize in files.items():
        batch.add_file(filename=filename, filesize=filesize)
    # Checking Batch __str__ method
    print(batch)
    out, err = capfd.readouterr()
    assert out == f"Batch of {sum(files.values())} bytes in {len(files)} {config_type} files\n" 

def test_batch_files():
    # Check `batch_files` handling of empty iterator
    assert [b for b in batch_files(files=[], threshold=0)] == []

    # Check `batch_files` handling of bad filenames
    files = [('test1',0),('test2',1)]
    assert [b for b in batch_files(files=files, threshold=0)] == []

    # Check `batch_files` handling of bad filenames
    files = [(None,0),(None,1)]
    assert [b for b in batch_files(files=files, threshold=0)] == []

    files = [
        ('https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz', 100_000),
    ]
    batches = [b for b in batch_files(files=files, threshold=1)]
    assert  len(batches) == 1

    files = [
        ('https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz', 1_000),
        ('https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz', 1_000),
    ]
    batches = [b for b in batch_files(files=files, threshold=1_000)]
    assert  len(batches) == 2
