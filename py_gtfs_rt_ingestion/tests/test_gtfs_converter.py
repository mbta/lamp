import os

from lib.convert_gtfs import GtfsConverter
from lib import ConfigType

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")


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

    config_type = ConfigType.from_filename(gtfs_schedule_file)
    converter = GtfsConverter(config_type)

    for prefix, table in converter.convert([gtfs_schedule_file]):
        table_name = prefix.lower()

        # check that we are expecting this name
        assert table_name in field_names.keys()  # pylint: disable=C0201

        # check that this table has all of the field names we're expecting
        for field in field_names[table_name]["column_names"]:  # type: ignore
            assert field in table.column_names

        # check that date information is in this table
        assert "timestamp" in table.column_names

        assert field_names[table_name]["length"] == table.num_rows
