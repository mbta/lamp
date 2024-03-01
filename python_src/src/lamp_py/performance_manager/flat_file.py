import os
import re
from typing import Set, List
from datetime import datetime

import sqlalchemy as sa
import pandas
import pyarrow

from lamp_py.aws.s3 import (
    file_list_from_s3,
    file_list_from_s3_with_details,
    upload_file,
)
from lamp_py.performance_manager.gtfs_utils import (
    static_version_key_from_service_date,
)
from lamp_py.postgres.rail_performance_manager_schema import (
    VehicleEvents,
    VehicleTrips,
    StaticStopTimes,
    StaticStops,
    TempEventCompare,
)
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger


class S3Archive:
    """
    Class for holding constant information about the public archive s3 bucket
    """

    BUCKET_NAME = os.environ.get("PUBLIC_ARCHIVE_BUCKET", "")
    RAIL_PERFORMANCE_PREFIX = os.path.join(
        "lamp", "subway-on-time-performance-v1"
    )
    INDEX_FILENAME = "index.csv"


def dates_to_update(db_manager: DatabaseManager) -> Set[datetime]:
    """
    Generate a list of service dates that we need to create / recreate flat
    files for. The list will include service dates that have data in the
    vehicle events table that are missing in the public archive bucket and
    service dates that we have generated new data for.
    """

    def db_service_dates_to_datetimes(df: pandas.DataFrame) -> Set[datetime]:
        """
        utility to convert a dataframe with a service_date column into a set of
        datetimes.
        """
        if df.size == 0:
            return set()

        return set(
            datetime.strptime(str(service_date), "%Y%m%d")
            for service_date in df["service_date"]
        )

    def filepaths_to_datetimes(filepaths: List[str]) -> Set[datetime]:
        """
        utility to convert filepaths to a set of datetimes

        archive filepaths are formatted like
        "s3://<bucket>/<prefix>/YYYY-MM-DD-subway-on-time-performance-v1.parquet"
        """
        # filename is everything after the last "/"
        # first 10 chars are YYYY-MM-DD
        # use strptime to convert to a datetime
        datetimes = set()
        for filepath in filepaths:
            match = re.search(r"(?P<date>\d{4}-\d{1,2}-\d{1,2})", filepath)
            if match is not None:
                datetimes.add(
                    datetime.strptime(match.group("date"), "%Y-%m-%d")
                )

        return datetimes

    # get the archived service dates as a set
    archive_filepaths = file_list_from_s3(
        bucket_name=S3Archive.BUCKET_NAME,
        file_prefix=S3Archive.RAIL_PERFORMANCE_PREFIX,
    )
    archived_datetimes = filepaths_to_datetimes(archive_filepaths)

    # get the processed service dates as a set
    vehicle_events_service_dates = db_manager.select_as_dataframe(
        sa.select(VehicleTrips.service_date).distinct()
    )
    processed_datetimes = db_service_dates_to_datetimes(
        vehicle_events_service_dates
    )

    # get service dates with new data from the last event loop
    new_data_service_dates = db_manager.select_as_dataframe(
        sa.select(TempEventCompare.service_date).distinct()
    )
    new_data_datetimes = db_service_dates_to_datetimes(new_data_service_dates)

    # return the processed dates that have yet to be archived plus dates with new data
    return (processed_datetimes - archived_datetimes).union(new_data_datetimes)


def write_flat_files(db_manager: DatabaseManager) -> None:
    """
    * find service dates that have not been fully archived
    * write flat files for those dates
    * update the archive log csv file
    """
    # if we don't have a public archive bucket, exit
    if S3Archive.BUCKET_NAME == "":
        return

    service_dates = dates_to_update(db_manager)

    # if no data to archive, early exit
    if len(service_dates) == 0:
        return

    process_logger = ProcessLogger(
        "bulk_flat_file_write", date_count=len(service_dates)
    )
    process_logger.log_start()

    for service_date in service_dates:
        sub_process_logger = ProcessLogger(
            "flat_file_write", service_date=service_date.strftime("%Y-%m-%d")
        )
        sub_process_logger.log_start()

        try:
            write_daily_table(db_manager=db_manager, service_date=service_date)
        except Exception as e:
            sub_process_logger.log_failure(e)
        else:
            sub_process_logger.log_complete()

    write_csv_index()

    process_logger.log_complete()


def write_csv_index() -> None:
    """
    write a csv file to the rail performance manager public archive describing
    all of the files in the archive including size, last modified, and service
    date.
    """
    file_details = file_list_from_s3_with_details(
        bucket_name=S3Archive.BUCKET_NAME,
        file_prefix=S3Archive.RAIL_PERFORMANCE_PREFIX,
    )

    df = pandas.DataFrame(file_details)

    # drop details for the index cvs and add in service date column
    df = df[~df["filepath"].str.endswith(S3Archive.INDEX_FILENAME)]
    df["service_date"] = df["filepath"].apply(lambda x: x.split("/")[-1][:10])

    # write to local csv and upload file to s3
    csv_path = "/tmp/rpm_archive_index.csv"
    df.to_csv(csv_path, index=False)

    upload_file(
        file_name=csv_path,
        object_path=os.path.join(
            S3Archive.BUCKET_NAME,
            S3Archive.RAIL_PERFORMANCE_PREFIX,
            S3Archive.INDEX_FILENAME,
        ),
    )

    os.remove(csv_path)


def write_daily_table(
    db_manager: DatabaseManager, service_date: datetime
) -> pyarrow.Table:
    """
    Generate a dataframe of all events and metrics for a single service date
    """
    service_date_int = int(service_date.strftime("%Y%m%d"))
    service_date_str = service_date.strftime("%Y-%m-%d")
    static_version_key = static_version_key_from_service_date(
        service_date=service_date_int, db_manager=db_manager
    )

    static_subquery = (
        sa.select(
            StaticStopTimes.arrival_time.label("scheduled_arrival_time"),
            StaticStopTimes.departure_time.label("scheduled_departure_time"),
            StaticStopTimes.schedule_travel_time_seconds.label(
                "scheduled_travel_time"
            ),
            StaticStopTimes.schedule_headway_branch_seconds.label(
                "scheduled_headway_branch"
            ),
            StaticStopTimes.schedule_headway_trunk_seconds.label(
                "scheduled_headway_trunk"
            ),
            StaticStopTimes.trip_id,
            sa.func.coalesce(
                StaticStops.parent_station,
                StaticStops.stop_id,
            ).label("parent_station"),
        )
        .select_from(StaticStopTimes)
        .join(
            StaticStops,
            sa.and_(
                StaticStopTimes.static_version_key
                == StaticStops.static_version_key,
                StaticStopTimes.stop_id == StaticStops.stop_id,
            ),
        )
        .where(
            StaticStopTimes.static_version_key == static_version_key,
            StaticStops.static_version_key == static_version_key,
        )
        .subquery(name="static_subquery")
    )

    select_query = (
        sa.select(
            VehicleEvents.stop_sequence,
            VehicleEvents.stop_id,
            VehicleEvents.parent_station,
            VehicleEvents.vp_move_timestamp.label("move_timestamp"),
            sa.func.coalesce(
                VehicleEvents.vp_stop_timestamp,
                VehicleEvents.tu_stop_timestamp,
            ).label("stop_timestamp"),
            VehicleEvents.travel_time_seconds,
            VehicleEvents.dwell_time_seconds,
            VehicleEvents.headway_trunk_seconds,
            VehicleEvents.headway_branch_seconds,
            VehicleEvents.service_date,
            VehicleTrips.route_id,
            VehicleTrips.direction_id,
            VehicleTrips.start_time,
            VehicleTrips.vehicle_id,
            VehicleTrips.branch_route_id,
            VehicleTrips.trunk_route_id,
            VehicleTrips.stop_count,
            VehicleTrips.trip_id,
            VehicleTrips.vehicle_label,
            VehicleTrips.vehicle_consist,
            VehicleTrips.direction,
            VehicleTrips.direction_destination,
            static_subquery.c.scheduled_arrival_time,
            static_subquery.c.scheduled_departure_time,
            static_subquery.c.scheduled_travel_time,
            static_subquery.c.scheduled_headway_branch,
            static_subquery.c.scheduled_headway_trunk,
        )
        .join(VehicleTrips, VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id)
        .join(
            static_subquery,
            sa.and_(
                static_subquery.c.trip_id == VehicleTrips.static_trip_id_guess,
                static_subquery.c.parent_station
                == VehicleEvents.parent_station,
            ),
            isouter=True,
        )
        .where(
            VehicleEvents.service_date == service_date_int,
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
    )

    flat_schema = pyarrow.schema(
        [
            ("stop_sequence", pyarrow.int16()),
            ("stop_id", pyarrow.string()),
            ("parent_station", pyarrow.string()),
            ("move_timestamp", pyarrow.int64()),
            ("stop_timestamp", pyarrow.int64()),
            ("travel_time_seconds", pyarrow.int64()),
            ("dwell_time_seconds", pyarrow.int64()),
            ("headway_trunk_seconds", pyarrow.int64()),
            ("headway_branch_seconds", pyarrow.int64()),
            ("service_date", pyarrow.int64()),
            ("route_id", pyarrow.string()),
            ("direction_id", pyarrow.bool_()),
            ("start_time", pyarrow.int64()),
            ("vehicle_id", pyarrow.string()),
            ("branch_route_id", pyarrow.string()),
            ("trunk_route_id", pyarrow.string()),
            ("stop_count", pyarrow.int16()),
            ("trip_id", pyarrow.string()),
            ("vehicle_label", pyarrow.string()),
            ("vehicle_consist", pyarrow.string()),
            ("direction", pyarrow.string()),
            ("direction_destination", pyarrow.string()),
            ("scheduled_arrival_time", pyarrow.int64()),
            ("scheduled_departure_time", pyarrow.int64()),
            ("scheduled_travel_time", pyarrow.int64()),
            ("scheduled_headway_branch", pyarrow.int64()),
            ("scheduled_headway_trunk", pyarrow.int64()),
        ]
    )

    # generate temp local and s3 paths from the service date
    filename = f"{service_date_str}-subway-on-time-performance-v1.parquet"
    temp_local_path = f"/tmp/{filename}"
    s3_path = os.path.join(
        S3Archive.BUCKET_NAME, S3Archive.RAIL_PERFORMANCE_PREFIX, filename
    )

    # the local path shouldn't exist, but make sure
    if os.path.exists(temp_local_path):
        os.remove(temp_local_path)

    # write the local file and upload it to s3
    db_manager.write_to_parquet(
        select_query=select_query,
        write_path=temp_local_path,
        schema=flat_schema,
    )

    upload_file(file_name=temp_local_path, object_path=s3_path)

    # delete the local file
    os.remove(temp_local_path)
