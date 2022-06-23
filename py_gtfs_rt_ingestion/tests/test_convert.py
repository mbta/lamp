import os
from unittest.mock import patch

from pyarrow import fs

from py_gtfs_rt_ingestion import Configuration
from py_gtfs_rt_ingestion import gz_to_pyarrow
from py_gtfs_rt_ingestion import ConfigType
from py_gtfs_rt_ingestion import zip_to_pyarrow

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


def test_bad_conversion() -> None:
    """
    test that bad filenames are returned back as the same strings. (these
    filepaths are going to be moved from incoming to error.)
    """
    # dummy config to avoid mypy errors
    config = Configuration(config_type=ConfigType.RT_ALERTS)

    bad_return = gz_to_pyarrow(filename="badfile", config=config)
    assert bad_return == "badfile"

    with patch("pyarrow.fs.S3FileSystem", return_value=fs.LocalFileSystem):
        bad_return = gz_to_pyarrow(filename="s3://badfile", config=config)
    assert bad_return == "s3://badfile"


def test_empty_files() -> None:
    """test that empty files produce empty tables"""
    configs_to_test = (
        ConfigType.RT_VEHICLE_POSITIONS,
        ConfigType.RT_ALERTS,
        ConfigType.RT_TRIP_UPDATES,
    )
    for config_type in configs_to_test:
        config = Configuration(config_type=config_type)

        empty_file = os.path.join(TEST_FILE_DIR, "empty.json.gz")
        table = gz_to_pyarrow(filename=empty_file, config=config)
        np_df = table.to_pandas()  # type: ignore
        assert np_df.shape == (0, len(config.export_schema))

        one_blank_file = os.path.join(TEST_FILE_DIR, "one_blank_record.json.gz")
        table = gz_to_pyarrow(filename=one_blank_file, config=config)
        np_df = table.to_pandas()  # type: ignore
        assert np_df.shape == (1, len(config.export_schema))


def test_vehicle_positions_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    rt_vehicle_positions_file = os.path.join(
        TEST_FILE_DIR,
        "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz",
    )
    config = Configuration(filename=rt_vehicle_positions_file)

    assert config.config_type == ConfigType.RT_VEHICLE_POSITIONS

    table = gz_to_pyarrow(filename=rt_vehicle_positions_file, config=config)
    np_df = table.to_pandas()  # type: ignore

    # tuple(na count, dtype, max, min)
    file_details = {
        "year": (0, "int16", "2022", "2022"),
        "month": (0, "int8", "1", "1"),
        "day": (0, "int8", "1", "1"),
        "hour": (0, "int8", "0", "0"),
        "feed_timestamp": (0, "int64", "1640995202", "1640995202"),
        "entity_id": (0, "object", "y4124", "1625"),
        "current_status": (0, "object", "STOPPED_AT", "INCOMING_AT"),
        "current_stop_sequence": (29, "float64", "640.0", "1.0"),
        "occupancy_percentage": (426, "float64", "nan", "nan"),
        "occupancy_status": (190, "object", "nan", "nan"),
        "stop_id": (29, "object", "nan", "nan"),
        "vehicle_timestamp": (0, "int64", "1640995198", "1640994366"),
        "bearing": (0, "int64", "360", "0"),
        "latitude": (0, "float64", "42.778499603271484", "41.82632064819336"),
        "longitude": (0, "float64", "-70.7895736694336", "-71.5481185913086"),
        "speed": (392, "float64", "29.9", "2.6"),
        "direction_id": (4, "float64", "1.0", "0.0"),
        "route_id": (0, "object", "Shuttle-Generic", "1"),
        "schedule_relationship": (0, "object", "UNSCHEDULED", "ADDED"),
        "start_date": (40, "object", "nan", "nan"),
        "start_time": (87, "object", "nan", "nan"),
        "trip_id": (426, "object", "nan", "nan"),
        "vehicle_id": (0, "object", "y4124", "1625"),
        "vehicle_label": (0, "object", "4124", "0420"),
        "vehicle_consist": (324, "object", "nan", "nan"),
    }

    # 426 records in 'entity' for 2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    assert np_df.shape == (426, len(config.export_schema))

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, upper, lower) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if upper != "nan":
            assert upper == str(np_df[col].max())
        if lower != "nan":
            assert lower == str(np_df[col].min())


def test_rt_alert_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    alerts_file = os.path.join(
        TEST_FILE_DIR,
        "2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz",
    )

    config = Configuration(filename=alerts_file)

    assert config.config_type == ConfigType.RT_ALERTS

    table = gz_to_pyarrow(filename=alerts_file, config=config)
    np_df = table.to_pandas()  # type: ignore

    # tuple(na count, dtype, max, min)
    file_details = {
        "year": (0, "int16", "2022", "2022"),
        "month": (0, "int8", "5", "5"),
        "day": (0, "int8", "4", "4"),
        "hour": (0, "int8", "15", "15"),
        "feed_timestamp": (0, "int64", "1651679986", "1651679986"),
        "entity_id": (0, "object", "442146", "293631"),
        "effect": (0, "object", "UNKNOWN_EFFECT", "DETOUR"),
        "effect_detail": (0, "object", "TRACK_CHANGE", "BIKE_ISSUE"),
        "cause": (0, "object", "UNKNOWN_CAUSE", "CONSTRUCTION"),
        "cause_detail": (0, "object", "UNKNOWN_CAUSE", "CONSTRUCTION"),
        "severity": (0, "int64", "10", "0"),
        "severity_level": (0, "object", "WARNING", "INFO"),
        "created_timestamp": (0, "int64", "1651679836", "1549051333"),
        "last_modified_timestamp": (0, "int64", "1651679848", "1549051333"),
        "alert_lifecycle": (0, "object", "UPCOMING_ONGOING", "NEW"),
        "duration_certainty": (0, "object", "UNKNOWN", "ESTIMATED"),
        "last_push_notification": (144, "float64", "nan", "nan"),
        "active_period": (2, "object", "nan", "nan"),
        "reminder_times": (132, "object", "nan", "nan"),
        "closed_timestamp": (142, "float64", "1651679848.0", "1651679682.0"),
        "short_header_text_translation": (0, "object", "nan", "nan"),
        "header_text_translation": (0, "object", "nan", "nan"),
        "description_text_translation": (0, "object", "nan", "nan"),
        "service_effect_text_translation": (0, "object", "nan", "nan"),
        "timeframe_text_translation": (44, "object", "nan", "nan"),
        "url_translation": (138, "object", "nan", "nan"),
        "recurrence_text_translation": (139, "object", "nan", "nan"),
        "informed_entity": (0, "object", "nan", "nan"),
    }

    # 144 records in 'entity' for 2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    assert np_df.shape == (144, len(config.export_schema))

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, upper, lower) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if upper != "nan":
            assert upper == str(np_df[col].max())
        if lower != "nan":
            assert lower == str(np_df[col].min())


def test_rt_trip_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    trip_updates_file = os.path.join(
        TEST_FILE_DIR,
        "2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz",
    )

    config = Configuration(filename=trip_updates_file)

    assert config.config_type == ConfigType.RT_TRIP_UPDATES

    table = gz_to_pyarrow(filename=trip_updates_file, config=config)
    np_df = table.to_pandas()  # type: ignore

    # tuple(na count, dtype, max, min)
    file_details = {
        "year": (0, "int16", "2022", "2022"),
        "month": (0, "int8", "5", "5"),
        "day": (0, "int8", "8", "8"),
        "hour": (0, "int8", "6", "6"),
        "feed_timestamp": (0, "int64", "1651989896", "1651989896"),
        "entity_id": (0, "object", "CR-532710-1518", "50922039"),
        "timestamp": (51, "float64", "1651989886.0", "1651989816.0"),
        "stop_time_update": (0, "object", "nan", "nan"),
        "direction_id": (0, "int64", "1", "0"),
        "route_id": (0, "object", "SL1", "1"),
        "start_date": (50, "object", "nan", "nan"),
        "start_time": (51, "object", "nan", "nan"),
        "trip_id": (0, "object", "CR-532710-1518", "50922039"),
        "route_pattern_id": (79, "object", "nan", "nan"),
        "schedule_relationship": (79, "object", "nan", "nan"),
        "vehicle_id": (39, "object", "nan", "nan"),
        "vehicle_label": (55, "object", "nan", "nan"),
    }

    # 79 records in 'entity' for
    # 2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    assert np_df.shape == (79, len(config.export_schema))

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, upper, lower) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if upper != "nan":
            assert upper == str(np_df[col].max())
        if lower != "nan":
            assert lower == str(np_df[col].min())


def test_bus_vehicle_positions_file_conversion() -> None:
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    rt_bus_vehicle_positions_file = os.path.join(
        TEST_FILE_DIR,
        "2022-05-05T16_00_15Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz",
    )
    config = Configuration(filename=rt_bus_vehicle_positions_file)

    assert config.config_type == ConfigType.BUS_VEHICLE_POSITIONS

    table = gz_to_pyarrow(filename=rt_bus_vehicle_positions_file, config=config)
    np_df = table.to_pandas()  # type: ignore

    # tuple(na count, dtype, max, min)
    file_details = {
        "year": (0, "int16", "2022", "2022"),
        "month": (0, "int8", "5", "5"),
        "day": (0, "int8", "5", "5"),
        "hour": (0, "int8", "16", "16"),
        "feed_timestamp": (0, "int64", "1651766414", "1651766414"),
        "entity_id": (0, "object", "1651766413_1740", "1651764730_1426"),
        "block_id": (484, "object", "nan", "nan"),
        "capacity": (12, "float64", "57.0", "36.0"),
        "current_stop_sequence": (844, "float64", "nan", "nan"),
        "load": (569, "float64", "42.0", "0.0"),
        "location_source": (0, "object", "transitmaster", "samsara"),
        "occupancy_percentage": (569, "float64", "80.0", "0.0"),
        "occupancy_status": (569, "object", "nan", "nan"),
        "revenue": (0, "bool", "True", "False"),
        "run_id": (484, "object", "nan", "nan"),
        "stop_id": (844, "object", "nan", "nan"),
        "vehicle_timestamp": (0, "int64", "1651766413", "1651764730"),
        "bearing": (0, "int64", "356", "0"),
        "latitude": (0, "float64", "42.65629769", "42.1069972"),
        "longitude": (0, "float64", "-70.62653102", "-71.272446339"),
        "speed": (163, "float64", "25.1189", "0.0"),
        "overload_id": (844, "float64", "nan", "nan"),
        "overload_offset": (844, "float64", "nan", "nan"),
        "route_id": (529, "object", "nan", "nan"),
        "schedule_relationship": (529, "object", "nan", "nan"),
        "start_date": (779, "object", "nan", "nan"),
        "trip_id": (529, "object", "nan", "nan"),
        "vehicle_id": (0, "object", "y3159", "y0408"),
        "vehicle_label": (0, "object", "3159", "0408"),
        "assignment_status": (358, "object", "nan", "nan"),
        "operator_id": (844, "object", "nan", "nan"),
        "logon_time": (484, "float64", "1651766401.0", "1651740838.0"),
        "name": (844, "object", "nan", "nan"),
        "first_name": (844, "object", "nan", "nan"),
        "last_name": (844, "object", "nan", "nan"),
    }

    # 844 records in 'entity' for 2022-05-05T16_00_15Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    assert np_df.shape == (844, len(config.export_schema))

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, upper, lower) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if upper != "nan":
            assert upper == str(np_df[col].max())
        if lower != "nan":
            assert lower == str(np_df[col].min())


def test_schedule_conversion() -> None:
    """
    test that a schedule zip file can be processed correctly, checking for files
    table names, table column names, and table lengths
    """
    gtfs_schedule_file = os.path.join(TEST_FILE_DIR, "MBTA_GTFS.zip")

    # list of expected files and their contents from
    # https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md
    #
    # lenghts were determined from manual inspection of the test file
    field_names = {
        "agency": {
            "length": 2,
            "column_names": [
                "agency_id",
                "agency_name",
                "agency_url",
                "agency_timezone",
                "agency_lang",
                "agency_phone",
            ],
        },
        "calendar": {
            "length": 121,
            "column_names": [
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ],
        },
        "calendar_attributes": {
            "length": 121,
            "column_names": [
                "service_id",
                "service_description",
                "service_schedule_name",
                "service_schedule_type",
                "service_schedule_typicality",
                "rating_start_date",
                "rating_end_date",
                "rating_description",
            ],
        },
        "calendar_dates": {
            "length": 68,
            "column_names": [
                "service_id",
                "date",
                "exception_type",
                "holiday_name",
            ],
        },
        "checkpoints": {
            "length": 638,
            "column_names": [
                "checkpoint_id",
                "checkpoint_name",
            ],
        },
        "directions": {
            "length": 412,
            "column_names": [
                "route_id",
                "direction_id",
                "direction",
                "direction_destination",
            ],
        },
        "facilities": {
            "length": 1746,
            "column_names": [
                "facility_id",
                "facility_code",
                "facility_class",
                "facility_type",
                "stop_id",
                "facility_short_name",
                "facility_long_name",
                "facility_desc",
                "facility_lat",
                "facility_lon",
                "wheelchair_facility",
            ],
        },
        "facilities_properties": {
            "length": 21372,
            "column_names": [
                "facility_id",
                "property_id",
                "value",
            ],
        },
        "facilities_properties_definitions": {
            "length": 28,
            "column_names": [
                "property_id",
                "definition",
                "possible_values",
            ],
        },
        "feed_info": {
            "length": 1,
            "column_names": [
                "feed_publisher_name",
                "feed_publisher_url",
                "feed_lang",
                "feed_start_date",
                "feed_end_date",
                "feed_version",
                "feed_contact_email",
            ],
        },
        "levels": {
            "length": 72,
            "column_names": [
                "level_id",
                "level_index",
                "level_name",
            ],
        },
        "lines": {
            "length": 139,
            "column_names": [
                "line_id",
                "line_short_name",
                "line_long_name",
                "line_desc",
                "line_url",
                "line_color",
                "line_text_color",
                "line_sort_order",
            ],
        },
        "linked_datasets": {
            "length": 3,
            "column_names": [
                "url",
                "trip_updates",
                "vehicle_positions",
                "service_alerts",
                "authentication_type",
            ],
        },
        "multi_route_trips": {
            "length": 7448,
            "column_names": [
                "added_route_id",
                "trip_id",
            ],
        },
        "pathways": {
            "length": 7782,
            "column_names": [
                "pathway_id",
                "from_stop_id",
                "to_stop_id",
                "facility_id",
                "pathway_mode",
                "is_bidirectional",
                "length",
                "wheelchair_length",
                "traversal_time",
                "wheelchair_traversal_time",
                "stair_count",
                "max_slope",
                "pathway_name",
                "pathway_code",
                "signposted_as",
                "instructions",
            ],
        },
        "routes": {
            "length": 236,
            "column_names": [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_desc",
                "route_fare_class",
                "route_type",
                "route_url",
                "route_color",
                "route_text_color",
                "route_sort_order",
                "line_id",
                "listed_route",
            ],
        },
        "route_patterns": {
            "length": 840,
            "column_names": [
                "route_pattern_id",
                "route_id",
                "direction_id",
                "route_pattern_name",
                "route_pattern_time_desc",
                "route_pattern_typicality",
                "route_pattern_sort_order",
                "representative_trip_id",
            ],
        },
        "shapes": {
            "length": 349445,
            "column_names": [
                "shape_id",
                "shape_pt_lat",
                "shape_pt_lon",
                "shape_pt_sequence",
                "shape_dist_traveled",
            ],
        },
        "stops": {
            "length": 9845,
            "column_names": [
                "stop_id",
                "stop_code",
                "stop_name",
                "stop_desc",
                "platform_code",
                "platform_name",
                "stop_lat",
                "stop_lon",
                "zone_id",
                "stop_address",
                "stop_url",
                "level_id",
                "location_type",
                "parent_station",
                "wheelchair_boarding",
                "municipality",
                "on_street",
                "at_street",
                "vehicle_type",
            ],
        },
        "stop_times": {
            "length": 2297343,
            "column_names": [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
                "stop_headsign",
                "pickup_type",
                "drop_off_type",
                "timepoint",
                "checkpoint_id",
                "continuous_pickup",
                "continuous_drop_off",
            ],
        },
        "transfers": {
            "length": 5468,
            "column_names": [
                "from_stop_id",
                "to_stop_id",
                "transfer_type",
                "min_transfer_time",
                "min_walk_time",
                "min_wheelchair_time",
                "suggested_buffer_time",
                "wheelchair_transfer",
                "from_trip_id",
                "to_trip_id",
            ],
        },
        "trips": {
            "length": 92277,
            "column_names": [
                "route_id",
                "service_id",
                "trip_id",
                "trip_headsign",
                "trip_short_name",
                "direction_id",
                "block_id",
                "shape_id",
                "wheelchair_accessible",
                "trip_route_type",
                "route_pattern_id",
                "bikes_allowed",
            ],
        },
    }

    for prefix, table in zip_to_pyarrow(gtfs_schedule_file):
        table_name = prefix.lower()

        # check that we are expecting this name
        assert table_name in field_names.keys()  # pylint: disable=C0201

        # check that this table has all of the field names we're expecting
        for field in field_names[table_name]["column_names"]:  # type: ignore
            assert field in table.column_names

        # check that date information is in this table
        assert "timestamp" in table.column_names

        assert field_names[table_name]["length"] == table.num_rows
