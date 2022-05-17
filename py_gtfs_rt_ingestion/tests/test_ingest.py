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
        "current_status",
        "current_stop_sequence",
        # "occupancy_status",
        "position.bearing",
        "position.latitude",
        "position.longitude",
        "stop_id",
        "trip.direction_id",
        "trip.route_id",
        "trip.schedule_relationship",
        "trip.start_date",
        "trip.start_time",
        "trip.trip_id",
        "vehicle_id",
        "vehicle_label",
        'vehicle_timestamp',
        # 'vehicle_consist_labels.list.item',
        "consist_labels.list.item",
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
