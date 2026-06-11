# pylint: disable=[R0913, R0914, R0917]
import os
from copy import deepcopy
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
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pyarrow import fs

from lamp_py.ingestion.config_busloc_trip import BusLocTripUpdateRecord
from lamp_py.ingestion.config_busloc_vehicle import BusLocVehicleRecord
from lamp_py.ingestion.config_rt_alerts import AlertsRecord
from lamp_py.ingestion.config_rt_trip import TripUpdateRecord
from lamp_py.ingestion.config_rt_vehicle import VehicleRecord
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.gtfs_rt_detail import FeedMessage
from lamp_py.ingestion.utils import override_schema
from lamp_py.runtime_utils.remote_files import LAMP, S3_SPRINGBOARD
from tests.test_resources import (
    incoming_dir,
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
    schema: type[FeedMessage], dy_gen: dy.random.Generator, timestamp: datetime
) -> dy.DataFrame[FeedMessage]:
    """Generate a GTFS-RT dataframe with a specified timestamp."""
    df = schema.sample(
        overrides={
            "header": {"timestamp": timestamp.timestamp(), "gtfs_realtime_version": "2.0", "incrementality": 0},
            "entity": schema.entity.sample(dy_gen),
        }
    )
    return df


def unnest_all_structs(table: pl.DataFrame) -> pl.DataFrame:
    """Flatten all struct types recursively."""
    while pl.Struct in table.dtypes:
        table = table.unnest(pl.selectors.struct(), separator=".")
    return table


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


@pytest.mark.parametrize(
    ["expected_config", "filepath_stub"],
    [
        (
            ConfigType.BUS_VEHICLE_POSITIONS,
            "https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced",
        ),
        (
            ConfigType.RT_ALERTS,
            "https_cdn.mbta.com_realtime_Alerts_enhanced",
        ),
        (
            ConfigType.RT_VEHICLE_POSITIONS,
            "https_cdn.mbta.com_realtime_VehiclePositions_enhanced",
        ),
        (
            ConfigType.RT_TRIP_UPDATES,
            "https_cdn.mbta.com_realtime_TripUpdates_enhanced",
        ),
        (
            ConfigType.BUS_TRIP_UPDATES,
            "com_prod_TripUpdates_enhanced",
        ),
    ],
    ids=["BUS_VEHICLE_POSITIONS", "RT_ALERTS", "RT_VEHICLE_POSITIONS", "RT_TRIP_UPDATES", "BUS_TRIP_UPDATES"],
)
def test_file_conversion(
    expected_config: ConfigType, filepath_stub: str, dy_gen: dy.random.Generator, tmp_path: Path
) -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    expected_converter = GtfsRtConverter(expected_config, metadata_queue=Queue())
    timestamp = datetime(2024, 1, 1, tzinfo=ZoneInfo("America/New_York"))

    df = expected_converter.detail.record_schema.sample(
        overrides={
            "header": {"timestamp": timestamp.timestamp(), "gtfs_realtime_version": "2.0", "incrementality": 0},
            "entity": expected_converter.detail.record_schema.entity.sample(dy_gen),
        }
    )

    incoming_file = tmp_path / (timestamp.isoformat() + filepath_stub + ".json.gz")

    df.write_ndjson(incoming_file, compression="gzip")

    config_type = ConfigType.from_filename(str(incoming_file))
    assert config_type == expected_config

    actual_converter = GtfsRtConverter(config_type, metadata_queue=Queue())
    actual_converter.thread_init()
    file_timestamp, filename, table = actual_converter.gz_to_pyarrow(str(incoming_file))

    assert file_timestamp
    assert file_timestamp.month == timestamp.month
    assert file_timestamp.year == timestamp.year
    assert file_timestamp.day == timestamp.day

    assert filename == str(incoming_file)

    assert table
    table = actual_converter.detail.transform_for_write(table)
    table = pl.DataFrame(
        table.cast(override_schema(table.schema, actual_converter.detail.table_schema.to_pyarrow_schema()))
    )
    assert (
        df.select(pl.col("entity").explode().struct.field("id").alias("id"))
        .unique()
        .join(table.select("id").unique(), on="id", how="anti")
        .height
        == 0
    ), "Some ids in the original message are missing from the converted table."

    assert not expected_converter.detail.table_schema.validate(table).is_empty()


class TestAlertsRecord(AlertsRecord):
    """AlertsRecord with a different effect_detail and cause_detail."""

    entity_inner = deepcopy(AlertsRecord.entity.inner.inner)  # type: ignore[attr-defined]
    entity_inner["alert"].inner["cause_detail"] = dy.String(nullable=True)
    entity_inner["alert"].inner["effect_detail"] = dy.String(nullable=True)

    entity = dy.List(inner=dy.Struct(inner=entity_inner), min_length=1)


@pytest.mark.parametrize(
    ["config_type", "input_schemas"],
    [
        (ConfigType.BUS_VEHICLE_POSITIONS, [BusLocVehicleRecord, BusLocVehicleRecord]),
        (ConfigType.RT_ALERTS, [AlertsRecord, AlertsRecord]),
        (ConfigType.RT_ALERTS, [TestAlertsRecord, AlertsRecord]),
        (ConfigType.RT_VEHICLE_POSITIONS, [VehicleRecord, VehicleRecord]),
        (ConfigType.RT_TRIP_UPDATES, [TripUpdateRecord, TripUpdateRecord]),
        (ConfigType.BUS_TRIP_UPDATES, [BusLocTripUpdateRecord, BusLocTripUpdateRecord]),
    ],
)
@pytest.mark.parametrize(
    "timestamp",
    [[datetime(2024, 1, 1, 0, 0, 1, tzinfo=ZoneInfo("UTC")), datetime(2024, 1, 1, 0, 0, 2, tzinfo=ZoneInfo("UTC"))]],
)
def test_convert(
    config_type: ConfigType,
    input_schemas: list[type[FeedMessage]],
    timestamp: list[datetime],
    dy_gen: dy.random.Generator,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """It ingests correctly with and without existing files."""
    monkeypatch.setattr("lamp_py.ingestion.convert_gtfs_rt.move_s3_objects", lambda files, __: files)
    monkeypatch.setattr("lamp_py.ingestion.convert_gtfs_rt.upload_file", create_mock_upload_file(tmp_path))
    dfs = []
    for i, ts in enumerate(timestamp):
        converter = GtfsRtConverter(config_type, Queue())
        df = gtfs_rt_factory(input_schemas[i], dy_gen, ts)

        incoming_file = tmp_path / f"{ts.isoformat()}.json.gz"

        df.write_ndjson(incoming_file, compression="gzip")

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

    flattened_records = []
    for df in dfs:
        flat = (
            df.select("entity", pl.col("header").struct.field("timestamp").alias("feed_timestamp"))
            .explode("entity")
            .unnest("entity")
            .unnest(pl.selectors.struct(), separator=".")
        )

        if "trip_update.stop_time_update" in flat.columns:
            flat = flat.explode("trip_update.stop_time_update")

        flattened_records.append(unnest_all_structs(flat))

    expected_records = pl.concat(flattened_records, how="diagonal")

    assert_frame_equal(converted_records, expected_records, check_row_order=False, check_column_order=False)
    converter.detail.table_schema.validate(converted_records)
