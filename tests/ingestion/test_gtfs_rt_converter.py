import gzip
import os
from datetime import datetime
from pathlib import Path
from queue import Queue
from zoneinfo import ZoneInfo

import msgspec
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from polyfactory.factories.msgspec_factory import MsgspecFactory

from lamp_py.ingestion.config_busloc_trip import BusTripUpdateMessage
from lamp_py.ingestion.config_busloc_vehicle import BusLocVehiclePositionMessage
from lamp_py.ingestion.config_rt_alerts import RtAlertMessage
from lamp_py.ingestion.config_rt_trip import RtTripUpdateMessage
from lamp_py.ingestion.config_rt_vehicle import RtVehiclePositionMessage
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.gtfs_rt_structs import FeedMessage
from lamp_py.utils.typing import struct_to_schema
from tests.test_resources import LocalS3Location


@pytest.mark.parametrize(
    ["record_schema", "config_type"],
    [
        (BusLocVehiclePositionMessage, ConfigType.BUS_VEHICLE_POSITIONS),
        (RtVehiclePositionMessage, ConfigType.RT_VEHICLE_POSITIONS),
        (RtTripUpdateMessage, ConfigType.RT_TRIP_UPDATES),
        (BusTripUpdateMessage, ConfigType.BUS_TRIP_UPDATES),
        (RtAlertMessage, ConfigType.RT_ALERTS),
    ],
)
@pytest.mark.parametrize(
    "timestamp",
    [[datetime(2024, 1, 1, 0, 0, 1, tzinfo=ZoneInfo("UTC")), datetime(2024, 1, 1, 0, 0, 2, tzinfo=ZoneInfo("UTC"))]],
)
def test_convert(
    record_schema: type[FeedMessage],
    config_type: ConfigType,
    timestamp: list[datetime],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """It ingests correctly with and without existing files."""
    test_dir = LocalS3Location(tmp_path.as_posix(), str(config_type))
    monkeypatch.setattr("lamp_py.ingestion.convert_gtfs_rt.move_s3_objects", lambda files, __: files)

    class RtFactory(MsgspecFactory[record_schema]):  # type: ignore[valid-type]
        """Factory to generate GTFS Realtime messages for testing."""

    records = []
    for ts in timestamp:
        record = RtFactory.build(header={"timestamp": int(ts.timestamp())})
        incoming_file = tmp_path / f"{ts.isoformat()}_file.json.gz"
        with gzip.open(incoming_file, "wb") as f:
            f.write(msgspec.json.encode(record))

        converter = GtfsRtConverter(config_type, Queue())
        monkeypatch.setattr(converter.detail, "remote_location", test_dir)
        converter.add_files([str(incoming_file)])
        converter.convert()
        records.append(record)

    expected_records = converter.detail.flatten_record(
        pl.read_json(
            msgspec.json.encode(records),
            schema=struct_to_schema(converter.detail.record_schema).to_polars_schema(),
        )
        .lazy()
        .explode("entity")
        .unnest()
        .with_columns(feed_timestamp=pl.col("timestamp"))
        .unnest(separator=".")
        .unnest(separator=".")
    )

    converted_records = pl.scan_parquet(
        [
            os.path.join(
                test_dir.s3_uri,
                ts.strftime("year=%Y/month=%-m/day=%-d/*.parquet"),
            )
            for ts in set(ts.date() for ts in timestamp)
        ]
    )

    assert_frame_equal(converted_records, expected_records, check_row_order=False, check_column_order=False)
