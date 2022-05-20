from pathlib import Path
import logging
import pyarrow.parquet as pq

from ingest import parseArgs, convert_json_to_parquet

LOGGER = logging.getLogger(__name__)
TEST_FILE_DIR = Path(__file__).parent.joinpath("test_files")

def test_argparse():
    """
    Test that the custom argparse is working as expected
    """
    dummy_file_path = 'my/fake/path'
    dummy_output_dir = 'my/fake/output_dir/'

    dummy_arguments = ['--input', dummy_file_path,
                       '--output', dummy_output_dir]
    args = parseArgs(dummy_arguments)

    assert args.input_file == dummy_file_path
    assert args.output_dir == dummy_output_dir

def test_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    rt_vehicle_positions_file = TEST_FILE_DIR.joinpath(
        "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz")

    convert_json_to_parquet(
        input_filename=rt_vehicle_positions_file,
        output_dir=tmpdir)

    # these are the columns one would expect from looking at the json data
    paths_from_json = set([
        "entity_id",
        "current_status",
        "current_stop_sequence",
        "occupancy_percentage",
        "occupancy_status",
        "stop_id",
        'vehicle_timestamp',
        "bearing",
        "latitude",
        "longitude",
        "speed",
        "direction_id",
        "route_id",
        "schedule_relationship",
        "start_date",
        "start_time",
        "trip_id",
        "vehicle_id",
        "vehicle_label",
        "vehicle_consist.list.item.label",
    ])

    # these are the columns that are added in for the header
    paths_from_header = set(["feed_timestamp"])

    all_expected_paths = paths_from_json | paths_from_header

    for filepath in Path(tmpdir).rglob('*.parquet'):
        parquet_file = pq.ParquetFile(filepath)
        parquet_schema = parquet_file.schema
        parquet_metadata = parquet_file.metadata

        found_paths = set()
        for item in parquet_schema:
            found_paths.add(item.path)

        # ensure all of the expected paths were found and there aren't any
        # additional ones
        assert all_expected_paths == found_paths

        # i counted 426 in the input json
        assert parquet_file.metadata.num_rows == 426

        # check that the partitioning worked correctly
        filepath_parts = filepath.parts
        assert "year=2022" in filepath_parts
        assert "month=1" in filepath_parts
        assert "day=1" in filepath_parts
        assert "hour=0" in filepath_parts

def test_rt_alert_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    alerts_file = TEST_FILE_DIR.joinpath(
        "2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz")

    convert_json_to_parquet(
        input_filename=alerts_file,
        output_dir=tmpdir)

    # these are the columns one would expect from looking at the json data
    paths_from_json = set([
        "entity_id",
        "effect",
        "effect_detail",
        "cause",
        "cause_detail",
        "severity",
        "severity_level",
        "created_timestamp",
        "last_modified_timestamp",
        "alert_lifecycle",
        "duration_certainty",
        "last_push_notification",
        "active_period",
        "reminder_times",
        "closed_timestamp",
        "short_header_text_translation",
        "header_text_translation",
        "description_text_translation",
        "service_effect_text_translation",
        "timeframe_text_translation",
        "url_translation",
        "recurrence_text_translation",
        "informed_entity",
    ])

    # these are the columns that are added in for the header
    paths_from_header = set(["feed_timestamp"])

    all_expected_paths = paths_from_json | paths_from_header

    for filepath in Path(tmpdir).rglob('*.parquet'):
        parquet_file = pq.ParquetFile(filepath)
        parquet_schema = parquet_file.schema
        parquet_metadata = parquet_file.metadata

        found_paths = set()
        for item in parquet_schema:
            # Only checking root paths...
            found_paths.add(item.path.split('.')[0])

        # ensure all of the expected paths were found and there aren't any
        # additional ones
        assert all_expected_paths == found_paths

        # # i counted 426 in the input json
        # assert parquet_file.metadata.num_rows == 426

        # check that the partitioning worked correctly
        filepath_parts = filepath.parts
        assert "year=2022" in filepath_parts
        assert "month=5" in filepath_parts
        assert "day=4" in filepath_parts
        assert "hour=15" in filepath_parts

def test_rt_trip_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    trip_update_file = TEST_FILE_DIR.joinpath(
        "2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz")

    convert_json_to_parquet(
        input_filename=trip_update_file,
        output_dir=tmpdir)

    # these are the columns one would expect from looking at the json data
    paths_from_json = set([
        "entity_id",
        "timestamp",
        "stop_time_update",
        "direction_id",
        "route_id",
        "start_date",
        "start_time",
        "trip_id",
        "route_pattern_id",
        "schedule_relationship",
        "vehicle_id",
        "vehicle_label",
    ])

    # these are the columns that are added in for the header
    paths_from_header = set(["feed_timestamp"])

    all_expected_paths = paths_from_json | paths_from_header

    for filepath in Path(tmpdir).rglob('*.parquet'):
        parquet_file = pq.ParquetFile(filepath)
        parquet_schema = parquet_file.schema
        parquet_metadata = parquet_file.metadata

        found_paths = set()
        for item in parquet_schema:
            # Only checking root paths...
            found_paths.add(item.path.split('.')[0])

        # ensure all of the expected paths were found and there aren't any
        # additional ones
        assert all_expected_paths == found_paths

        # # i counted 426 in the input json
        # assert parquet_file.metadata.num_rows == 426

        # check that the partitioning worked correctly
        filepath_parts = filepath.parts
        assert "year=2022" in filepath_parts
        assert "month=5" in filepath_parts
        assert "day=8" in filepath_parts
        assert "hour=6" in filepath_parts