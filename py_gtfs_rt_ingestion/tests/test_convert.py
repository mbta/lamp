from pathlib import Path

from py_gtfs_rt_ingestion import Configuration, convert_files

TEST_FILE_DIR = Path(__file__).parent.joinpath("test_files")

def test_vehicle_positions_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    rt_vehicle_positions_file = TEST_FILE_DIR.joinpath(
        "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz")
    config = Configuration(filename=rt_vehicle_positions_file.name)

    table = convert_files(filepaths=[rt_vehicle_positions_file],
                          config=config)

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
        "vehicle_consist",
    ])

    # these are the columns that are added in for the header
    paths_from_header = set([
        "feed_timestamp",
        "year",
        "month",
        "day",
        "hour",
    ])

    all_expected_paths = paths_from_json | paths_from_header

    found_paths = set()
    for item in table.schema.names:
        found_paths.add(item)

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == found_paths

    # i counted 426 in the input json
    assert table.num_rows == 426

def test_rt_alert_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    alerts_file = TEST_FILE_DIR.joinpath(
        "2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz")

    config = Configuration(filename=alerts_file.name)
    table = convert_files(filepaths=[alerts_file], config=config)

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
    paths_from_header = set([
        "feed_timestamp",
        "year",
        "month",
        "day",
        "hour",
    ])

    all_expected_paths = paths_from_json | paths_from_header

    found_paths = set()
    for item in table.schema.names:
        found_paths.add(item)

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == found_paths

    # # i counted 426 in the input json
    # assert parquet_file.metadata.num_rows == 426

def test_rt_trip_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    trip_updates_file = TEST_FILE_DIR.joinpath(
        "2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz")

    config = Configuration(filename=trip_updates_file.name)
    table = convert_files(filepaths=[trip_updates_file], config=config)

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
    paths_from_header = set([
        "feed_timestamp",
        "year",
        "month",
        "day",
        "hour",
    ])

    all_expected_paths = paths_from_json | paths_from_header

    found_paths = set()
    for item in table.schema.names:
        found_paths.add(item)

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == found_paths

    # # i counted 426 in the input json
    # assert parquet_file.metadata.num_rows == 426
