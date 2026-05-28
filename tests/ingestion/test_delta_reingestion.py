from datetime import date
from queue import Queue
from unittest.mock import MagicMock, patch

import pytest

from lamp_py.ingestion.backfill.delta_reingestion import delta_reingestion_runner
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3_INCOMING, S3Location


@pytest.mark.parametrize(
    "bucket,start_date,end_date,object_exists_side_effect,expect_error,expected_convert_calls,expected_file_list_calls",
    [
        pytest.param("bad-bucket", date(2026, 5, 1), date(2026, 5, 1), [False], True, 0, 0, id="invalid-bucket"),
        pytest.param(S3_ARCHIVE, date(2026, 5, 1), date(2026, 5, 2), [True, False], False, 1, 1, id="skip-then-run"),
        pytest.param(S3_INCOMING, date(2026, 5, 1), date(2026, 5, 1), [False], False, 1, 1, id="single-day-run"),
    ],
)
# pylint: disable=too-many-arguments,too-many-positional-arguments
def test_delta_reingestion_runner(
    bucket: str,
    start_date: date,
    end_date: date,
    object_exists_side_effect: list[bool],
    expect_error: bool,
    expected_convert_calls: int,
    expected_file_list_calls: int,
) -> None:
    """Test delta_reingestion_runner routes files and handles validation."""
    converter = MagicMock(spec=GtfsRtConverter)
    converter.config_type = ConfigType.RT_TRIP_UPDATES

    with (
        patch(
            "lamp_py.ingestion.backfill.delta_reingestion.object_exists", side_effect=object_exists_side_effect
        ) as mock_exists,
        patch(
            "lamp_py.ingestion.backfill.delta_reingestion.file_list_from_s3", return_value=["file-a", "file-b"]
        ) as mock_list,
    ):
        if expect_error:
            with pytest.raises(ValueError):
                delta_reingestion_runner(
                    start_date=start_date,
                    end_date=end_date,
                    final_output_path=S3Location(S3_ARCHIVE, "lamp/RT_TRIP_UPDATES"),
                    converter=converter,
                    bucket=bucket,
                )
        else:
            delta_reingestion_runner(
                start_date=start_date,
                end_date=end_date,
                final_output_path=S3Location(S3_ARCHIVE, "lamp/RT_TRIP_UPDATES"),
                converter=converter,
                bucket=bucket,
            )

    assert converter.convert.call_count == expected_convert_calls
    assert converter.add_files.call_count == expected_convert_calls
    assert converter.clean_local_folders.call_count == expected_convert_calls
    assert converter.reset_files.call_count == expected_convert_calls
    assert mock_list.call_count == expected_file_list_calls
    if not expect_error:
        assert mock_exists.call_count == len(object_exists_side_effect)


@pytest.mark.parametrize(
    "initial_files,expected_files",
    [
        pytest.param([], [], id="empty-list"),
        pytest.param(["a.json.gz", "b.json.gz"], [], id="clears-non-empty-list"),
    ],
)
def test_reset_files(initial_files: list[str], expected_files: list[str]) -> None:
    """Test reset_files clears the converter's file list."""
    converter = GtfsRtConverter(config_type=ConfigType.RT_ALERTS, metadata_queue=Queue())
    converter.add_files(initial_files)
    converter.reset_files()
    assert converter.files == expected_files
