# pylint: disable=[R0913, R0917]
import os
from datetime import datetime
from pathlib import Path
from queue import Queue
from shutil import copy2
from typing import (
    Callable,
    Optional,
)
from unittest.mock import patch
from zoneinfo import ZoneInfo

import dataframely as dy
import pandas
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pyarrow import fs

from lamp_py.ingestion.config_busloc_vehicle import BusLocVehicleRecord, BusLocVehicleTable, GTFSRealtime
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import flatten_table_schema
from lamp_py.runtime_utils.remote_files import LAMP, S3_SPRINGBOARD

from ..test_resources import (
    incoming_dir,
    test_files_dir,
)


def create_mock_upload_file(tmp_path: Path) -> Callable[[str, str, Optional[dict]], bool]:
    """Create a mock upload_file function that copies files to tmp_path instead of S3."""

    def mock_upload_file(file_name: str, object_path: str, _: dict | None = None) -> bool:
        """Copy file to tmp_path instead of uploading to S3."""
        dest = tmp_path / Path(object_path.replace("s3://", ""))
        dest.parent.mkdir(parents=True, exist_ok=True)
        copy2(file_name, dest)
        return True

    return mock_upload_file


def gtfs_rt_factory(
    schema: type[GTFSRealtime], dy_gen: dy.random.Generator, timestamp: datetime
) -> dy.DataFrame[GTFSRealtime]:
    """Generate an infinite stream of GTFS-RT dataframes that conform to the provided schema."""
    df = schema.sample(
        overrides={
            "header": {"timestamp": timestamp.timestamp(), "gtfs_realtime_version": "2.0", "incrementality": 0},
            "entity": schema.entity.sample(dy_gen),
        }
    )
    return df


def test_bad_conversion_local() -> None:
    """
    Test that a bad filename will result in an empty table and the filename will
    be added to the error files list
    """
    # dummy config to avoid mypy errors
    converter = GtfsRtConverter(config_type=ConfigType.RT_ALERTS, metadata_queue=Queue())
    converter.add_files(["badfile"])

    # process the bad file and get the table out
    for _ in converter.process_files():
        assert False, "Generated Table for s3 badfile"

    # archive files should be empty, error files should have the bad file
    assert len(converter.archive_files) == 0
    assert converter.error_files == ["badfile"]


def test_bad_conversion_s3() -> None:
    """
    Test that a bad s3 filename will result in an empty table and the filename
    will be added to the error files list
    """
    with patch("pyarrow.fs.S3FileSystem", return_value=fs.LocalFileSystem):
        converter = GtfsRtConverter(config_type=ConfigType.RT_ALERTS, metadata_queue=Queue())
        converter.add_files(["s3://badfile"])

        # process the bad file and get the table out
        for _ in converter.process_files():
            assert False, "Generated Table for s3 badfile"

        # archive files should be empty, error files should have the bad file
        assert len(converter.archive_files) == 0
        assert converter.error_files == ["badfile"]


def test_empty_files() -> None:
    """Test that empty files produce empty tables"""
    configs_to_test = (
        ConfigType.RT_VEHICLE_POSITIONS,
        ConfigType.RT_ALERTS,
        ConfigType.RT_TRIP_UPDATES,
    )

    for config_type in configs_to_test:
        converter = GtfsRtConverter(config_type=config_type, metadata_queue=Queue())
        converter.thread_init()

        empty_file = os.path.join(incoming_dir, "empty.json.gz")
        _, filename, table = converter.gz_to_pyarrow(empty_file)
        assert filename == empty_file
        assert table.to_pandas().shape == (
            0,
            len(converter.detail.import_schema) + 4,  # add 4 for header timestamp columns
        )

        one_blank_file = os.path.join(incoming_dir, "one_blank_record.json.gz")
        _, filename, table = converter.gz_to_pyarrow(one_blank_file)
        assert filename == one_blank_file
        assert table.to_pandas().shape == (
            1,
            len(converter.detail.import_schema) + 4,  # add 4 for header timestamp columns
        )


def drop_list_columns(table: pandas.DataFrame) -> pandas.DataFrame:
    """
    Drop any columns with list objects to perform dataframe compare
    """
    list_columns = (
        "hour",
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
    assert table.num_columns == len(converter.detail.import_schema) + 4  # add 4 for header timestamp columns

    np_df = flatten_table_schema(table).to_pandas()
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

    np_df = flatten_table_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_GTFS-RT_VP_OLD.parquet")
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
    assert table.num_columns == len(converter.detail.import_schema) + 4  # add 4 for header timestamp columns

    np_df = flatten_table_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_GTFS-RT_ALERT.parquet")
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
    assert table.num_columns == len(converter.detail.import_schema) + 4  # add 4 for header timestamp columns

    np_df = flatten_table_schema(table).to_pandas()
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

    np_df = flatten_table_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_GTFS-RT_TU_OLD.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


def test_bus_vehicle_positions_file_conversion(dy_gen: dy.random.Generator, tmp_path: Path) -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    vehicle_timestamp = datetime(2026, 1, 1, tzinfo=ZoneInfo("America/New_York"))  # a valid datetime
    df = BusLocVehicleRecord.sample(
        overrides={
            "header": {"timestamp": vehicle_timestamp.timestamp(), "gtfs_realtime_version": "2.0", "incrementality": 0},
            "entity": BusLocVehicleRecord.entity.sample(dy_gen),
        }
    )

    incoming_file = (
        tmp_path
        / f"{vehicle_timestamp.isoformat()}_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz"
    )

    df.write_ndjson(incoming_file, compression="gzip")

    config_type = ConfigType.from_filename(str(incoming_file))
    assert config_type == ConfigType.BUS_VEHICLE_POSITIONS

    converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    converter.thread_init()
    timestamp, filename, table = converter.gz_to_pyarrow(str(incoming_file))

    assert timestamp
    assert timestamp.month == vehicle_timestamp.month
    assert timestamp.year == vehicle_timestamp.year
    assert timestamp.day == vehicle_timestamp.day

    assert filename == str(incoming_file)

    assert table
    table = pl.DataFrame(flatten_table_schema(table))
    assert (
        df.select(pl.col("entity").explode().struct.field("id").alias("id"))
        .unique()
        .join(table.select("id").unique(), on="id", how="anti")
        .height
        == 0
    ), "Some ids in the original message are missing from the converted table."

    assert not BusLocVehicleTable.validate(table).is_empty()


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
    assert table.num_columns == len(converter.detail.import_schema) + 4  # add 4 for header timestamp columns

    np_df = flatten_table_schema(table).to_pandas()
    np_df = drop_list_columns(np_df)

    parquet_file = os.path.join(test_files_dir, "ingestion_BUSLOC_TU.parquet")
    parquet_df = pandas.read_parquet(parquet_file)
    parquet_df = drop_list_columns(parquet_df)

    compare_result = np_df.compare(parquet_df, align_axis=1)
    assert compare_result.shape[0] == 0, f"{compare_result}"


@pytest.mark.parametrize(["schema", "config_type"], [(BusLocVehicleRecord, ConfigType.BUS_VEHICLE_POSITIONS)])
@pytest.mark.parametrize(
    "timestamp",
    [[datetime(2024, 1, 1, 0, 0, 1, tzinfo=ZoneInfo("UTC")), datetime(2024, 1, 1, 0, 0, 2, tzinfo=ZoneInfo("UTC"))]],
)
def test_convert(
    schema: type[GTFSRealtime],
    config_type: ConfigType,
    timestamp: list[datetime],
    dy_gen: dy.random.Generator,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """It ingests correctly with and without existing files."""
    monkeypatch.setattr("lamp_py.ingestion.convert_gtfs_rt.move_s3_objects", lambda files, __: files)
    monkeypatch.setattr("lamp_py.ingestion.convert_gtfs_rt.upload_file", create_mock_upload_file(tmp_path))
    dfs = []
    for ts in timestamp:
        df = gtfs_rt_factory(schema, dy_gen, ts)

        incoming_file = tmp_path / f"{ts.isoformat()}.json.gz"

        df.write_ndjson(incoming_file, compression="gzip")

        converter = GtfsRtConverter(config_type, Queue())
        monkeypatch.setattr(converter, "tmp_folder", tmp_path.as_posix())
        converter.add_files([str(incoming_file)])
        converter.convert()
        dfs.append(df)

    converted_records = pl.read_parquet(
        [
            tmp_path.joinpath(
                S3_SPRINGBOARD,
                LAMP,
                str(config_type),
                ts.strftime("year=%Y/month=%-m/day=%-d/%Y-%m-%dT00:00:00.parquet"),
            ).as_posix()
            for ts in set(ts.date() for ts in timestamp)
        ]
    )

    expected_records = (
        pl.union(dfs)
        .select("entity", pl.col("header").struct.field("timestamp").alias("feed_timestamp"))
        .explode("entity")
        .unnest("entity")
        .unnest("vehicle", separator=".")
        .unnest("vehicle.vehicle", "vehicle.position", "vehicle.trip", "vehicle.operator", separator=".")
    )

    assert_frame_equal(converted_records, expected_records, check_row_order=False, check_column_order=False)
