import os
from typing import List

import duckdb

from lamp_py.aws.s3 import upload_file
from lamp_py.runtime_utils import remote_files as rf
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.lamp_exception import EmptyDataStructureException
from lamp_py.runtime_utils.process_logger import ProcessLogger

VIEWS: dict[str, list[rf.S3Location]] = {
    "/year=*/month=*/day=*/*.parquet": [  # year-month-day partitioned directories
        rf.springboard_rt_vehicle_positions,
        rf.springboard_rt_trip_updates,
        rf.springboard_devgreen_rt_vehicle_positions,
        rf.springboard_devgreen_rt_trip_updates,
        rf.springboard_devgreen_lrtp_trip_updates,
        rf.springboard_lrtp_trip_updates,
        rf.rt_alerts,
        rf.bus_vehicle_positions,
        rf.bus_trip_updates,
    ],
    "/*.parquet": [  # timestamp- or date-partitioned directories
        rf.tm_stop_crossing,
        rf.tm_daily_work_piece,
        rf.tm_daily_logged_message,
    ],
    "": [
        rf.tm_daily_sched_adherence_waiver_file,
        rf.tm_geo_node_file,
        rf.tm_route_file,
        rf.tm_trip_file,
        rf.tm_vehicle_file,
        rf.tm_operator_file,
        rf.tm_run_file,
        rf.tm_block_file,
        rf.tm_work_piece_file,
        rf.tm_time_point_file,
        rf.tm_pattern_geo_node_xref_file,
        rf.glides_operator_signed_in,
        rf.glides_trips_updated,
        rf.tableau_bus_all,
        rf.tableau_bus_operator_mapping_all,
        rf.public_alerts_file,
        rf.tableau_rt_vehicle_positions_lightrail_60_day,
        rf.tableau_rt_trip_updates_heavyrail_30_day,
        rf.tableau_devgreen_rt_trip_updates_lightrail_60_day,
        rf.tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    ],
}


def authenticate(connection: duckdb.DuckDBPyConnection) -> bool:
    """Register IAM credentials with duckdb."""
    connection.install_extension("aws")
    connection.load_extension("aws")
    return connection.sql(  # type: ignore[index]
        """
    CREATE SECRET IF NOT EXISTS secret (
        TYPE s3,
        PROVIDER credential_chain
    );
    """
    ).fetchone()[0]


def build_view(
    connection: duckdb.DuckDBPyConnection,
    view_name: str,
    data_location: rf.S3Location,
    partition_strategy: str = "",
) -> bool:
    """Create view using data location according to partitions."""
    pl = ProcessLogger("build_view")

    view_target = f"{data_location.s3_uri}{partition_strategy}"
    pl.add_metadata(view_name=view_name, view_target=view_target)

    try:
        connection.from_parquet(view_target, hive_partitioning=True).create_view(view_name)
        return True
    except Exception as e:
        pl.log_failure(e)

    pl.log_complete()

    return False


def register_read_ymd(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Register function in DuckDB using SQL text."""
    pl = ProcessLogger("register_read_ymd")
    pl.log_start()
    connection.sql(
        """
        CREATE OR REPLACE MACRO read_ymd

            (directory_name, start_date, end_date, bucket := 's3://mbta-ctd-dataplatform-springboard') AS TABLE (
             SELECT *
             FROM read_parquet(
                list_transform(
                    range(
                        start_date,
                        end_date,
                        INTERVAL 1 DAY
                    ),
                    lambda x : strftime(
                        concat_ws(
                            '/',
                            bucket,
                            'lamp',
                            directory_name,
                            'year=%Y/month=%-m/day=%-d/%xT%H:%M:%S.parquet'
                        ),
                        x
                    )
                ),
                hive_partitioning = True
            )
            )
        """
    )

    pl.log_complete()


def register_gtfs_service_id_table(
    connection: duckdb.DuckDBPyConnection,
) -> bool:
    """Register a view in DuckDB where each row represents a service_id active on a service date."""
    pl = ProcessLogger("register_gtfs_service_id_table")
    pl.log_start()
    try:
        connection.sql(
            """
            CREATE OR REPLACE TABLE service_ids AS
            SELECT
            service_date,
            list(service_id) as service_ids
            FROM
            (
                SELECT
                generate_series::DATE as service_date,
                service_id
                FROM
                generate_series(make_date(2009, 3, 21), current_date + 90, INTERVAL '1 DAY')
                LEFT JOIN (
                    UNPIVOT read_parquet('s3://mbta-performance/lamp/gtfs_archive/*/calendar.parquet') ON monday,
                    tuesday,
                    wednesday,
                    thursday,
                    friday,
                    saturday,
                    sunday INTO NAME dayofweek VALUE enabled
                ) c ON generate_series::DATE BETWEEN strptime(start_date::text, '%Y%m%d')::date AND strptime(end_date::text, '%Y%m%d')::date
                AND enabled = 1
                AND lower(strftime(generate_series, '%A')) = dayofweek
                AND generate_series::date BETWEEN strptime(c.gtfs_active_date::text, '%Y%m%d')::date and strptime(c.gtfs_end_date::text, '%Y%m%d')::date
                ANTI JOIN read_parquet('s3://mbta-performance/lamp/gtfs_archive/*/calendar_dates.parquet') removed ON generate_series::date BETWEEN strptime(removed.gtfs_active_date::text, '%Y%m%d')::date and strptime(removed.gtfs_end_date::text, '%Y%m%d')::date
                AND generate_series::date = strptime(removed.date::text, '%Y%m%d')::date
                AND exception_type = 2
                UNION
                SELECT
                generate_series::DATE,
                service_id
                FROM
                generate_series(make_date(2009, 3, 14), current_date + 90, INTERVAL '1 DAY')
                INNER JOIN read_parquet('s3://mbta-performance/lamp/gtfs_archive/*/calendar_dates.parquet') added ON generate_series::date = strptime(added.date::text, '%Y%m%d')::date
                AND added.exception_type = 1
            )
            GROUP BY
            service_date
            ORDER BY
            service_date
            """
        )
        pl.log_complete()
        return True
    except duckdb.Error as e:
        pl.log_failure(e)
        return False


def register_read_schedule(
    connection: duckdb.DuckDBPyConnection,
) -> bool:
    """Register a view in DuckDB where each row represents a stop in a trip on a service date."""
    pl = ProcessLogger("register_read_schedule")
    pl.log_start()
    try:
        connection.sql(
            """
            CREATE OR REPLACE MACRO read_schedule (start_date, end_date) AS TABLE ( -- multi-date variant
            SELECT
                d.service_date,
                t.service_id,
                t.trip_id,
                t.route_id,
                t.direction_id,
                t.trip_headsign,
                st.stop_sequence,
                s.stop_id,
                s.stop_name,
                service_date + st.departure_time::INTERVAL as departure_dt
            FROM
                lamp.service_ids d
                LEFT JOIN read_parquet(
                list_transform(
                    generate_series(year(start_date), year(end_date)),
                    lambda x: printf('s3://mbta-performance/lamp/gtfs_archive/%d/trips.parquet', x)
                )
                ) t ON d.service_date BETWEEN strptime(t.gtfs_active_date::text, '%Y%m%d')::date AND strptime(t.gtfs_end_date::text, '%Y%m%d')::date
                AND t.service_id in d.service_ids
                LEFT JOIN read_parquet(
                list_transform(
                    generate_series(year(start_date), year(end_date)),
                    lambda x: printf('s3://mbta-performance/lamp/gtfs_archive/%d/stop_times.parquet', x)
                )
                ) st ON d.service_date BETWEEN strptime(st.gtfs_active_date::text, '%Y%m%d')::date AND strptime(st.gtfs_end_date::text, '%Y%m%d')::date
                AND t.trip_id = st.trip_id
                LEFT JOIN read_parquet(
                list_transform(
                    generate_series(year(start_date), year(end_date)),
                    lambda x: printf('s3://mbta-performance/lamp/gtfs_archive/%d/stops.parquet', x)
                )
                ) s ON d.service_date BETWEEN strptime(s.gtfs_active_date::text, '%Y%m%d')::date AND strptime(s.gtfs_end_date::text, '%Y%m%d')::date
                AND st.stop_id = s.stop_id
            WHERE
                d.service_date BETWEEN start_date AND end_date
            ORDER BY
                service_date,
                route_id,
                t.trip_id,
                stop_sequence
            ),
            (service_date) AS TABLE ( -- single-date variant
            SELECT
                d.service_date,
                t.service_id,
                t.trip_id,
                t.route_id,
                t.direction_id,
                t.trip_headsign,
                st.stop_sequence,
                s.stop_id,
                s.stop_name,
                service_date + st.departure_time::INTERVAL as departure_dt
            FROM
                lamp.service_ids d
                LEFT JOIN read_parquet(
                printf('s3://mbta-performance/lamp/gtfs_archive/%d/trips.parquet', year(service_date))
                ) t ON service_date BETWEEN strptime(t.gtfs_active_date::text, '%Y%m%d')::date AND strptime(t.gtfs_end_date::text, '%Y%m%d')::date
                AND t.service_id in d.service_ids
                LEFT JOIN read_parquet(
                printf('s3://mbta-performance/lamp/gtfs_archive/%d/stop_times.parquet', year(service_date))
                ) st ON service_date BETWEEN strptime(st.gtfs_active_date::text, '%Y%m%d')::date AND strptime(st.gtfs_end_date::text, '%Y%m%d')::date
                AND t.trip_id = st.trip_id
                LEFT JOIN read_parquet(
                printf('s3://mbta-performance/lamp/gtfs_archive/%d/stops.parquet', year(service_date))
                ) s ON service_date BETWEEN strptime(s.gtfs_active_date::text, '%Y%m%d')::date AND strptime(s.gtfs_end_date::text, '%Y%m%d')::date
                AND st.stop_id = s.stop_id
            WHERE
                d.service_date = service_date
            ORDER BY
                service_date,
                route_id,
                t.trip_id,
                stop_sequence
            )
            """
        )
        pl.log_complete()
        return True
    except duckdb.Error as e:
        pl.log_failure(e)
        return False


def add_views_to_local_schema(
    connection: duckdb.DuckDBPyConnection, views: List[rf.S3Location], partition_strategy: str = ""
) -> List[str]:
    """Add views of remote Parquet files to duckdb schema."""
    built_views: List[str] = []
    for item in views:
        view_name = os.path.splitext(os.path.basename(item.prefix))[0]
        result = build_view(connection, view_name, item, partition_strategy)
        if result:
            built_views.append(view_name)

    if not built_views:
        raise EmptyDataStructureException

    return built_views


def pipeline(  # pylint: disable=dangerous-default-value
    views: dict[str, list[rf.S3Location]] = VIEWS,
    local_location: str = "/tmp/lamp.db",
    remote_location: rf.S3Location | None = rf.lightswitch,
) -> None:
    """Create duckdb metastore and upload to specified location."""
    pl = ProcessLogger("lightswitch.pipeline", local_location=local_location)
    pl.log_start()

    os.environ["SERVICE_NAME"] = "lightswitch"

    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "PUBLIC_ARCHIVE_BUCKET",
            "ARCHIVE_BUCKET",
        ],
    )

    with duckdb.connect(local_location) as con:
        auth = authenticate(con)
        pl.add_metadata(authenticated=auth)
        for k, v in views.items():
            add_views_to_local_schema(con, v, k)
        register_read_ymd(con)
        register_gtfs_service_id_table(con)
        register_read_schedule(con)

    if remote_location:
        pl.add_metadata(remote_location=remote_location.s3_uri)
        upload_file(local_location, remote_location.s3_uri)
        os.remove(local_location)

    pl.log_complete()
