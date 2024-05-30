import os
from queue import Queue
from unittest.mock import patch

from pyarrow import fs
import pandas

from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import flatten_schema

from ..test_resources import (
    incoming_dir,
    test_files_dir,
)


def test_bad_conversion_local() -> None:
    """
    test that a bad filename will result in an empty table and the filename will
    be added to the error files list
    """
    # dummy config to avoid mypy errors
    converter = GtfsRtConverter(
        config_type=ConfigType.RT_ALERTS, metadata_queue=Queue()
    )
    converter.add_files(["badfile"])

    # process the bad file and get the table out
    for _ in converter.process_files():
        assert False, "Generated Table for s3 badfile"

    # archive files should be empty, error files should have the bad file
    assert len(converter.archive_files) == 0
    assert converter.error_files == ["badfile"]


def test_bad_conversion_s3() -> None:
    """
    test that a bad s3 filename will result in an empty table and the filename
    will be added to the error files list
    """
    with patch("pyarrow.fs.S3FileSystem", return_value=fs.LocalFileSystem):
        converter = GtfsRtConverter(
            config_type=ConfigType.RT_ALERTS, metadata_queue=Queue()
        )
        converter.add_files(["s3://badfile"])

        # process the bad file and get the table out
        for _ in converter.process_files():
            assert False, "Generated Table for s3 badfile"

        # archive files should be empty, error files should have the bad file
        assert len(converter.archive_files) == 0
        assert converter.error_files == ["badfile"]


def test_empty_files() -> None:
    """test that empty files produce empty tables"""
    configs_to_test = (
        ConfigType.RT_VEHICLE_POSITIONS,
        ConfigType.RT_ALERTS,
        ConfigType.RT_TRIP_UPDATES,
    )

    for config_type in configs_to_test:
        converter = GtfsRtConverter(
            config_type=config_type, metadata_queue=Queue()
        )
        converter.thread_init()

        empty_file = os.path.join(incoming_dir, "empty.json.gz")
        _, filename, table = converter.gz_to_pyarrow(empty_file)
        assert filename == empty_file
        assert table.to_pandas().shape == (
            0,
            len(converter.detail.import_schema)
            + 5,  # add 5 for header timestamp columns
        )

        one_blank_file = os.path.join(incoming_dir, "one_blank_record.json.gz")
        _, filename, table = converter.gz_to_pyarrow(one_blank_file)
        assert filename == one_blank_file
        assert table.to_pandas().shape == (
            1,
            len(converter.detail.import_schema)
            + 5,  # add 5 for header timestamp columns
        )


def drop_list_columns(table: pandas.DataFrame) -> pandas.DataFrame:
    """
    drop any columns with list objects to perform dataframe compare
    """
    list_columns = (
        "consist",
        "translation",
        "informed_entity",
        "active_period",
        "reminder_times",
        "stop_time_update",
        "multi_carriage_details",
    )
    for column in table.columns:
        for list_col in list_columns:
            if list_col in column:
                table.drop(columns=column, inplace=True)
    return table


def test_vehicle_positions_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    gtfs_rt_file = os.path.join(
        incoming_dir,
        "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz",
    )
    config_type = ConfigType.from_filename(gtfs_rt_file)
    assert config_type == ConfigType.RT_VEHICLE_POSITIONS

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(gtfs_rt_file)

    assert timestamp.month == 1
    assert timestamp.year == 2022
    assert timestamp.day == 1

    assert filename == gtfs_rt_file

    # 426 records in 'entity' for 2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    assert table.num_rows == 426
    assert (
        table.num_columns == len(converter.detail.import_schema) + 5
    )  # add 5 for header timestamp columns

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_GTFS-RT_VP.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"

    # check that we are able to handle older vp files from 16 sept 2019 - 4 march 2020
    old_gtfs_rt_file = os.path.join(
        incoming_dir,
        "2019-12-12T00_00_10_https___mbta_gtfs_s3_dev.s3.amazonaws.com_concentrate_VehiclePositions_enhanced.json",
    )
    timestamp, filename, table = converter.gz_to_pyarrow(old_gtfs_rt_file)

    assert timestamp.month == 12
    assert timestamp.year == 2019
    assert timestamp.day == 12

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(
        test_files_dir, "ingestion_GTFS-RT_VP_OLD.parquet"
    )
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


def test_rt_alert_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    gtfs_rt_file = os.path.join(
        incoming_dir,
        "2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz",
    )

    config_type = ConfigType.from_filename(gtfs_rt_file)
    assert config_type == ConfigType.RT_ALERTS

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(gtfs_rt_file)

    assert timestamp.month == 5
    assert timestamp.year == 2022
    assert timestamp.day == 4

    assert filename == gtfs_rt_file

    # 144 records in 'entity' for 2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    assert table.num_rows == 144
    assert (
        table.num_columns == len(converter.detail.import_schema) + 5
    )  # add 5 for header timestamp columns

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(
        test_files_dir, "ingestion_GTFS-RT_ALERT.parquet"
    )
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


def test_rt_trip_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    gtfs_rt_file = os.path.join(
        incoming_dir,
        "2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz",
    )

    config_type = ConfigType.from_filename(gtfs_rt_file)
    assert config_type == ConfigType.RT_TRIP_UPDATES

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(gtfs_rt_file)

    assert timestamp.month == 5
    assert timestamp.year == 2022
    assert timestamp.day == 8

    assert filename == gtfs_rt_file

    # 79 records in 'entity' for
    # 2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    assert table.num_rows == 79
    assert (
        table.num_columns == len(converter.detail.import_schema) + 5
    )  # add 5 for header timestamp columns

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_GTFS-RT_TU.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"

    # check that we are able to handle older vp files from 16 sept 2019 - 4 march 2020
    old_gtfs_rt_file = os.path.join(
        incoming_dir,
        "2019-12-12T00_00_57_https___mbta_gtfs_s3_dev.s3.amazonaws.com_concentrate_TripUpdates_enhanced.json",
    )
    timestamp, filename, table = converter.gz_to_pyarrow(old_gtfs_rt_file)
    assert timestamp.month == 12
    assert timestamp.year == 2019
    assert timestamp.day == 12

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(
        test_files_dir, "ingestion_GTFS-RT_TU_OLD.parquet"
    )
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


def test_bus_vehicle_positions_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    gtfs_rt_file = os.path.join(
        incoming_dir,
        "2022-05-05T16_00_15Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz",
    )

    config_type = ConfigType.from_filename(gtfs_rt_file)
    assert config_type == ConfigType.BUS_VEHICLE_POSITIONS

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(gtfs_rt_file)

    assert timestamp.month == 5
    assert timestamp.year == 2022
    assert timestamp.day == 5

    assert filename == gtfs_rt_file

    # 844 records in 'entity' for 2022-05-05T16_00_15Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    assert table.num_rows == 844
    assert (
        table.num_columns == len(converter.detail.import_schema) + 5
    )  # add 5 for header timestamp columns

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_BUSLOC_VP.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


def test_bus_trip_updates_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    gtfs_rt_file = os.path.join(
        incoming_dir,
        "2022-06-28T10_03_18Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz",
    )

    config_type = ConfigType.from_filename(gtfs_rt_file)
    assert config_type == ConfigType.BUS_TRIP_UPDATES

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(gtfs_rt_file)

    assert timestamp.month == 6
    assert timestamp.year == 2022
    assert timestamp.day == 28

    assert filename == gtfs_rt_file

    # 157 records in 'entity' for 2022-06-28T10_03_18Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
    assert table.num_rows == 157
    assert (
        table.num_columns == len(converter.detail.import_schema) + 5
    )  # add 5 for header timestamp columns

    np_df = flatten_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_BUSLOC_TU.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"
