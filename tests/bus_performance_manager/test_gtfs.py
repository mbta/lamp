import os
import logging
from unittest import mock
from urllib import request

import polars as pl
import polars.testing as pl_test

from lamp_py.bus_performance_manager.gtfs import gtfs_events_for_date

current_dir = os.path.join(os.path.dirname(__file__))

SERVICE_DATE = 20240801

mock_file_list = [
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/calendar.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/calendar_dates.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/directions.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/feed_info.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/route_patterns.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/routes.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/stop_times.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/stops.parquet",
    "https://performancedata.mbta.com/lamp/gtfs_archive/2024/trips.parquet",
]


def mock_file_download(object_path: str, file_name: str) -> bool:
    """dummy download function for https files"""
    request.urlretrieve(object_path, file_name)
    return True


@mock.patch("lamp_py.bus_performance_manager.gtfs.file_list_from_s3")
@mock.patch(
    "lamp_py.bus_performance_manager.gtfs.download_file", new=mock_file_download
)
def test_gtfs_events_for_date(s3_patch: mock.MagicMock) -> None:
    """
    test gtfs_events_for_date pipeline
    """
    # mock files from S3 with https://performancedata paths
    s3_patch.return_value = mock_file_list

    bus_events = gtfs_events_for_date(SERVICE_DATE)

    # CSV Bus events
    expected_bus_events = pl.read_csv(
        os.path.join(current_dir, "bus_test_gtfs.csv"),
        schema=bus_events.schema,
    ).sort(by=["trip_id", "stop_sequence"])
    # CSV trips
    expected_trips = expected_bus_events.select("trip_id").unique()

    # Filter and sort pipeline events for CSV trips
    bus_events = bus_events.join(
        expected_trips, on="trip_id", how="inner"
    ).sort(by=["trip_id", "stop_sequence"])

    # Compare pipeline values to CSV values by column
    column_exceptions = []
    for column in expected_bus_events.columns:
        for trip_id in expected_bus_events.get_column("trip_id").unique():
            try:
                pl_test.assert_series_equal(
                    bus_events.filter(
                        (pl.col("trip_id") == trip_id)
                    ).get_column(column),
                    expected_bus_events.filter(
                        (pl.col("trip_id") == trip_id)
                    ).get_column(column),
                )
            except Exception as exception:
                logging.error(
                    "Process values (column=%s - trip_id=%s) do not match bus_test_gtfs.csv",
                    column,
                    trip_id,
                )
                logging.exception(exception)
                column_exceptions.append(exception)

    # will only raise one column exception, but error logging will print all columns with issues
    if column_exceptions:
        raise column_exceptions[0]
