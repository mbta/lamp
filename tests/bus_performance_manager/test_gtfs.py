import os
import logging
from unittest import mock
from dataclasses import dataclass
from datetime import date

import polars as pl
import polars.testing as pl_test

from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_events_for_date
from lamp_py.runtime_utils.remote_files import GTFSArchive

current_dir = os.path.join(os.path.dirname(__file__))

SERVICE_DATE = date(2024, 8, 1)

gtfs = GTFSArchive(bucket="https://performancedata.mbta.com", prefix="lamp/gtfs_archive")


@dataclass
class S3Location:
    """
    wrapper for a bucket name and prefix pair used to define an s3 location
    """

    bucket: str
    prefix: str
    version: str = "1.0"

    @property
    def s3_uri(self) -> str:
        """generate the full s3 uri for the location"""
        return f"{self.bucket}/{self.prefix}"


@mock.patch("lamp_py.utils.gtfs_utils.object_exists")
@mock.patch("lamp_py.utils.gtfs_utils.compressed_gtfs", gtfs)
@mock.patch("lamp_py.runtime_utils.remote_files.S3Location", S3Location)
def test_gtfs_events_for_date(exists_patch: mock.MagicMock) -> None:
    """
    test gtfs_events_for_date pipeline
    """
    # mock files from S3 with https://performancedata paths
    exists_patch.return_value = True

    bus_events = bus_gtfs_events_for_date(SERVICE_DATE)

    # CSV Bus events
    expected_bus_events = pl.read_csv(
        os.path.join(current_dir, "bus_test_gtfs.csv"),
        schema=bus_events.schema,
    ).sort(by=["plan_trip_id", "stop_sequence"])
    # CSV trips
    expected_trips = expected_bus_events.select("plan_trip_id").unique()

    # Filter and sort pipeline events for CSV trips
    bus_events = bus_events.join(expected_trips, on="plan_trip_id", how="right").sort(
        by=["plan_trip_id", "stop_sequence"]
    )

    for trip_id in bus_events.get_column("plan_trip_id").unique():
        trip = bus_events.filter((pl.col("plan_trip_id") == trip_id))
        # try:
        # the first stop does not have a "travel to time" - so it must be null
        assert trip["plan_travel_time_seconds"].head(1).is_null()[0]

        # # only the first is null
        assert trip["plan_travel_time_seconds"].null_count() == 1
        # # and all the rest are 0 or greater
        assert (trip["plan_travel_time_seconds"] >= 0).count() == trip["plan_travel_time_seconds"].len() - 1
        # and they're not all zero
        assert (trip["plan_travel_time_seconds"] == 0).sum() != trip["plan_travel_time_seconds"].len() - 1

    # plan_route_direction_headway_seconds
    for _, group in bus_events.group_by(["stop_id", "direction_id", "route_id"]):
        assert (group["plan_route_direction_headway_seconds"] >= 0).count() == group[
            "plan_route_direction_headway_seconds"
        ].is_not_null().sum()

    # plan_direction_destination_headway_seconds
    for _, group in bus_events.group_by(["stop_id", "direction_destination"]):
        assert (group["plan_direction_destination_headway_seconds"] >= 0).count() == group[
            "plan_direction_destination_headway_seconds"
        ].is_not_null().sum()

        # breakpoint()
    # Compare pipeline values to CSV values by column
    column_exceptions = []
    #
    # Tempoarily skip headway columns as random sorting is causing non-deterministic
    # results with these test values
    #

    skip_columns = (
        "plan_route_direction_headway_seconds",
        "plan_direction_destination_headway_seconds",
    )
    for column in expected_bus_events.columns:
        if column in skip_columns:
            continue
        for trip_id in expected_bus_events.get_column("plan_trip_id").unique():
            try:
                trip = bus_events.filter((pl.col("plan_trip_id") == trip_id))

                pl_test.assert_series_equal(
                    trip.get_column(column),
                    expected_bus_events.filter((pl.col("plan_trip_id") == trip_id)).get_column(column),
                )
            except Exception as exception:
                logging.error(
                    "Process values (column=%s - plan_trip_id=%s) do not match bus_test_gtfs.csv",
                    column,
                    trip_id,
                )
                logging.exception(exception)
                column_exceptions.append(exception)

    # will only raise one column exception, but error logging will print all columns with issues
    if column_exceptions:
        raise column_exceptions[0]
