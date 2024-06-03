import os
from queue import Queue
from typing import Callable, Iterator, Optional, List, Dict, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pyarrow import Table

from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.convert_gtfs import GtfsConverter

from ..test_resources import incoming_dir


def all_table_attributes() -> Dict:
    """
    # list of expected files and their contents from
    # https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md
    #
    # lengths were determined from manual inspection of the test file
    """
    return {
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


@pytest.fixture(name="_set_env_vars")
def fixture_set_env_vars() -> None:
    """setup bucket names for this test"""
    os.environ["SPRINGBOARD_BUCKET"] = "springboard"
    os.environ["ERROR_BUCKET"] = "error"
    os.environ["ARCHIVE_BUCKET"] = "archive"


@pytest.fixture(name="_s3_patch")
def fixture_s3_patch(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """
    insert a monkeypatch over the s3 functions used by the gtfs converter.

    * write parquet file function patched to check the table rather than write it
    * move s3 objects asserts that all moves are to archive and not error
    """

    # keep track of what tables are written to check against after converting
    tables_written: List[str] = []

    def mock_write_parquet_file(
        table: Table,
        file_type: str,
        s3_dir: str,
        partition_cols: List[str],
        visitor_func: Optional[Callable[..., None]] = None,
    ) -> None:
        """
        instead of writing the parquet file to s3, inspect the contents of the
        table. call the visitor function on a dummy s3 path.
        """
        # pull the name out of the s3 path and check that we are expecting this table
        table_name = file_type.lower()
        tables_written.append(table_name)

        table_attributes = all_table_attributes()[table_name]
        # check that this table has all of the field names we're expecting
        for field in table_attributes["column_names"]:
            assert field in table.column_names

        # check that date information is in this table and that we're partitioning it
        assert "timestamp" in table.column_names
        assert partition_cols == ["timestamp"]

        assert table_attributes["length"] == table.num_rows

        # call the visitor function, which should add the string to the queue to check later
        if visitor_func is not None:
            visitor_func(os.path.join(s3_dir, "written.parquet"))

    monkeypatch.setattr(
        "lamp_py.ingestion.convert_gtfs.write_parquet_file",
        mock_write_parquet_file,
    )

    def mock_gtfs_files_to_convert() -> List[Tuple[str, int]]:
        """provide list of gtfs paths to convert"""
        return [(os.path.join(incoming_dir, "MBTA_GTFS.zip"), 1655517536)]

    monkeypatch.setattr(
        "lamp_py.ingestion.convert_gtfs.gtfs_files_to_convert",
        mock_gtfs_files_to_convert,
    )

    # everything before this yield is executed before running the test
    yield
    # everything after this yield is executed after running the test

    # check that we "wrote" all the tables we expected to write and that the
    # feed info table was written last
    tables_expected = set(all_table_attributes().keys())
    assert tables_expected == set(tables_written)
    assert tables_written[-1] == "feed_info"


def test_schedule_conversion(
    _set_env_vars: Callable[..., None], _s3_patch: Callable[..., None]
) -> None:
    """
    test that a schedule zip file can be processed correctly, checking for files
    table names, table column names, and table lengths
    """
    # generate a schedule converter with an empty queue to inspect later
    metadata_queue: Queue = Queue()
    converter = GtfsConverter(
        config_type=ConfigType.SCHEDULE, metadata_queue=metadata_queue
    )

    # pass in the test schedule file and convert. the monkey patched parquet
    # writer function will check that all tables generated by conversion were
    # processed correctly.
    converter.convert()

    # check through all of the paths that were "written" to s3 have the
    # appropriate format and that we "wrote" all of the tables.
    while not metadata_queue.empty():
        s3_path = metadata_queue.get(block=False)

        assert "springboard" in s3_path
        assert "FEED_INFO" in s3_path
        assert "written.parquet" in s3_path
