import os

import sqlalchemy as sa
import pyarrow

from lamp_py.aws.s3 import write_parquet_file
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


def write_flat_files(db_manager: DatabaseManager) -> None:
    """write flat files to s3 for datetimes"""
    # if we don't have a public archive bucket, exit
    if os.environ.get("PUBLIC_ARCHIVE_BUCKET", "") == "":
        return

    date_df = db_manager.select_as_dataframe(
        sa.select(TempEventCompare.service_date).distinct()
    )

    # if no data has been processed, exit
    if date_df.shape[0] == 0:
        return

    process_logger = ProcessLogger(
        "bulk_flat_file_write", date_count=date_df.shape[0]
    )
    process_logger.log_start()

    s3_directory = os.path.join(
        os.environ["PUBLIC_ARCHIVE_BUCKET"], "lamp", "flat_file"
    )

    for date in date_df["service_date"]:
        sub_process_logger = ProcessLogger("flat_file_write", service_date=date)
        sub_process_logger.log_start()

        try:
            as_str = str(date)
            filename = f"{as_str[0:4]}-{as_str[4:6]}-{as_str[6:8]}-rail-performance-{{i}}.parquet"

            flat_table = generate_daily_table(db_manager, date)
            sub_process_logger.add_metadata(row_count=flat_table.shape[0])

            write_parquet_file(
                table=flat_table,
                file_type="flat_rail_performance",
                s3_dir=s3_directory,
                partition_cols=["year", "month", "day"],
                basename_template=filename,
            )
        except Exception as e:
            sub_process_logger.log_failure(e)
        else:
            sub_process_logger.log_complete()

    process_logger.log_complete()


def generate_daily_table(
    db_manager: DatabaseManager, service_date: int
) -> pyarrow.Table:
    """
    Generate a dataframe of all events and metrics for a single service date
    """
    static_version_key = static_version_key_from_service_date(
        service_date=service_date, db_manager=db_manager
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

    query = (
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
            VehicleEvents.service_date == service_date,
            sa.or_(
                VehicleEvents.vp_move_timestamp.is_not(None),
                VehicleEvents.vp_stop_timestamp.is_not(None),
            ),
        )
    )

    # get the days events as a dataframe from postgres
    days_events = db_manager.select_as_dataframe(query)

    # transform the seru
    days_events["year"] = (
        days_events["service_date"].astype(str).str[:4].astype(int)
    )
    days_events["month"] = (
        days_events["service_date"].astype(str).str[4:6].astype(int)
    )
    days_events["day"] = (
        days_events["service_date"].astype(str).str[6:8].astype(int)
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
            ("direction_id", pyarrow.int8()),
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
            ("year", pyarrow.int16()),
            ("month", pyarrow.int8()),
            ("day", pyarrow.int8()),
        ]
    )

    return pyarrow.Table.from_pandas(days_events, schema=flat_schema)
