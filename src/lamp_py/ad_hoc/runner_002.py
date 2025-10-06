import os
from datetime import date, datetime
from datetime import timedelta
import sqlalchemy as sa

import pyarrow
import pyarrow.parquet as pq

from lamp_py.performance_manager.flat_file import S3Archive
from lamp_py.performance_manager.gtfs_utils import static_version_key_from_service_date
from lamp_py.postgres.rail_performance_manager_schema import (
    StaticRoutes,
    StaticStopTimes,
    StaticStops,
    VehicleEvents,
    VehicleTrips,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.postgres.postgres_utils import DatabaseIndex, DatabaseManager, start_rds_writer_process
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE
from lamp_py.runtime_utils.remote_files import LAMP
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD
from lamp_py.aws.s3 import file_list_from_s3, upload_file
from lamp_py.aws.s3 import download_file
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType


def write_daily_table_adhoc_cr_only(db_manager: DatabaseManager, service_date: datetime) -> pyarrow.Table:
    """
    Generate a dataframe of all events and metrics for a single service date
    """
    service_date_int = int(service_date.strftime("%Y%m%d"))
    service_date_str = service_date.strftime("%Y-%m-%d")
    static_version_key = static_version_key_from_service_date(service_date=service_date_int, db_manager=db_manager)

    static_subquery = (
        sa.select(
            StaticStopTimes.arrival_time.label("scheduled_arrival_time"),
            StaticStopTimes.departure_time.label("scheduled_departure_time"),
            StaticStopTimes.schedule_travel_time_seconds.label("scheduled_travel_time"),
            StaticStopTimes.schedule_headway_branch_seconds.label("scheduled_headway_branch"),
            StaticStopTimes.schedule_headway_trunk_seconds.label("scheduled_headway_trunk"),
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
                StaticStopTimes.static_version_key == StaticStops.static_version_key,
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
        .select_from(VehicleEvents)
        .join(VehicleTrips, VehicleEvents.pm_trip_id == VehicleTrips.pm_trip_id)
        .join(
            static_subquery,
            sa.and_(
                static_subquery.c.trip_id == VehicleTrips.static_trip_id_guess,
                static_subquery.c.parent_station == VehicleEvents.parent_station,
            ),
            isouter=True,
        )
        .join(
            StaticRoutes,
            sa.and_(
                VehicleTrips.route_id == StaticRoutes.route_id,
                VehicleTrips.static_version_key == StaticRoutes.static_version_key,
            ),
        )
        .where(
            VehicleEvents.service_date == service_date_int,
            VehicleTrips.static_version_key == static_version_key,
            StaticRoutes.route_type == 3,
            VehicleTrips.revenue == sa.true(),
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
        .order_by(
            sa.func.coalesce(
                VehicleEvents.vp_move_timestamp,
                VehicleEvents.vp_stop_timestamp,
                VehicleEvents.tu_stop_timestamp,
            )
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
    filename = f"{service_date_str}-commuter-rail-on-time-performance-v1.parquet"
    temp_local_path = f"/tmp/{filename}"
    s3_path = os.path.join(S3Archive.BUCKET_NAME, os.path.join(LAMP, "commuter-rail-on-time-performance-v1"), filename)

    # the local path shouldn't exist, but make sure
    if os.path.exists(temp_local_path):
        os.remove(temp_local_path)

    # write the local file and upload it to s3
    db_manager.write_to_parquet(
        select_query=select_query,
        write_path=temp_local_path,
        schema=flat_schema,
    )

    upload_file(
        file_name=temp_local_path,
        object_path=s3_path,
        extra_args={"Metadata": {S3Archive.VERSION_KEY: S3Archive.RPM_VERSION}},
    )

    # delete the local file
    os.remove(temp_local_path)


# pylint: enable=R0801


def write_flat_files(db_manager: DatabaseManager) -> None:

    process_logger = ProcessLogger("adhoc_bulk_flat_file_write")
    process_logger.log_start()

    try:
        service_dates = []
        start_date = datetime(year=2025, month=9, day=21)
        end_date = datetime(year=2025, month=9, day=30)

        # add 1 for inclusive
        date_diff_days = (start_date - end_date).days * -1 + 1

        for i in range(0, date_diff_days):
            service_dates.append(start_date + timedelta(days=i))
        # get the service dates that need to be archived

        process_logger.add_metadata(date_count=len(service_dates))

        # if no data to archive, early exit
        if len(service_dates) == 0:
            process_logger.log_complete()
            return

        for service_date in service_dates:
            sub_process_logger = ProcessLogger(
                "adhoc_flat_file_write",
                service_date=service_date.strftime("%Y-%m-%d"),
            )
            sub_process_logger.log_start()

            try:
                write_daily_table_adhoc_cr_only(db_manager=db_manager, service_date=service_date)
            except Exception as e:
                sub_process_logger.log_failure(e)
            else:
                sub_process_logger.log_complete()

        process_logger.log_complete()

    except Exception as e:
        process_logger.log_failure(e)


def runner() -> None:
    rpm_db_manager = DatabaseManager(db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER)

    write_flat_files(rpm_db_manager)
