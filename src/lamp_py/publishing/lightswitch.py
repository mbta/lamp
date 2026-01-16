import os
from typing import List

import duckdb
from lamp_py.runtime_utils import remote_files as rf
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.lamp_exception import EmptyDataStructureException
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import upload_file

VIEWS: list[tuple[str, list[rf.S3Location], str]] = [
    (
        "gtfs_rt",
        [  # year-month-day partitioned directories
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
        "/year=*/month=*/day=*/*.parquet",
    ),
    (
        "transit_master",
        [  # timestamp- or date-partitioned directories
            rf.tm_stop_crossing,
            rf.tm_daily_work_piece,
            rf.tm_daily_logged_message,
        ],
        "/*.parquet",
    ),
    (
        "transit_master",
        [
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
        ],
        "",
    ),
    (
        "glides",
        [
            rf.glides_operator_signed_in,
            rf.glides_trips_updated,
        ],
        "",
    ),
    (
        "bus",
        [
            rf.tableau_bus_all,
            rf.tableau_bus_operator_mapping_all,
        ],
        "",
    ),
    (
        "alerts",
        [
            rf.public_alerts_file,
        ],
        "",
    ),
    (
        "rail",
        [
            rf.tableau_rt_vehicle_positions_lightrail_60_day,
            rf.tableau_rt_trip_updates_heavyrail_30_day,
            rf.tableau_devgreen_rt_trip_updates_lightrail_60_day,
            rf.tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
        ],
        "",
    ),
]


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


def create_schemas(
    connection: duckdb.DuckDBPyConnection,
    schemas: list[str],
) -> list[str]:
    """Create the specified schemas in the local database."""
    pl = ProcessLogger("create_schemas")
    pl.log_start()
    built_schemas = []
    for schema_name in schemas:
        try:
            connection.sql(f"CREATE SCHEMA {schema_name}")
            built_schemas.append(schema_name)
        except duckdb.Error as e:
            pl.log_warning(e)
    pl.log_complete()
    return built_schemas


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


def register_effective_gtfs_timestamps(
    connection: duckdb.DuckDBPyConnection,
    s3_prefix: str = "s3://mbta-ctd-dataplatform-springboard/lamp",
) -> bool:
    """Register a table in DuckDB that shows the effective GTFS Schedule timestamp for each service date."""
    pl = ProcessLogger("register_effective_gtfs_timestamps")
    pl.log_start()
    schema_name = "gtfs_schedule"
    create_schemas(connection, [schema_name])
    try:
        connection.sql(
            f"""
            CREATE OR REPLACE TABLE {schema_name}.effective_timestamps AS
            SELECT
                service_date,
                split_part(rating, ' ', 2) as rating_year,
                split_part(rating, ' ', 1) as rating_season,
                effective_timestamp
            FROM
            (
                SELECT
                unnest AS service_date,
                max(timestamp) as effective_timestamp,
                max_by(split_part(feed_version, ',', 1), timestamp) as rating
            FROM
                read_parquet(
                    '{s3_prefix}/FEED_INFO/timestamp=*/*.parquet',
                    hive_partitioning = True
                )
            INNER JOIN
                unnest(list_transform(generate_series('2019-01-01'::DATE, current_date() + 60, INTERVAL '1 DAY'), LAMBDA x: x :: DATE))
            ON
                to_timestamp(timestamp)::DATE < unnest AND
                unnest BETWEEN strptime(feed_start_date::TEXT, '%Y%m%d')::DATE AND strptime(feed_end_date::TEXT, '%Y%m%d')::DATE
            GROUP BY
                unnest
            ORDER BY
                service_date
            )
            """
        )
        pl.log_complete()
        return True
    except duckdb.Error as e:
        pl.log_failure(e)
        return False


def register_gtfs_schedule_view(
    connection: duckdb.DuckDBPyConnection,
    s3_prefix: str = "s3://mbta-ctd-dataplatform-springboard/lamp",
) -> bool:
    """Register a view in DuckDB where each row represents a stop in a trip on a service date."""
    pl = ProcessLogger("register_effective_gtfs_timestamps")
    pl.log_start()
    schema_name = "gtfs_schedule"
    create_schemas(connection, [schema_name])
    try:
        connection.sql(
            f"""
            CREATE OR REPLACE VIEW {schema_name}.date_trip_sequence AS
            SELECT
                service_date,
                t.service_id,
                t.trip_id,
                t.route_id,
                t.trip_headsign,
                st.stop_sequence,
                s.stop_id,
                s.stop_name,
                service_date + st.departure_time::INTERVAL as departure_dt
            FROM
                gtfs_schedule.effective_timestamps
            INNER JOIN
                read_parquet(
                    '{s3_prefix}/CALENDAR_DATES/timestamp=*/*.parquet',
                    hive_partitioning = True
                ) c
            ON
                effective_timestamp = c.timestamp
                AND service_date = strptime(date::text, '%Y%m%d')::date
            INNER JOIN
                read_parquet(
                    '{s3_prefix}/TRIPS/timestamp=*/*.parquet',
                    hive_partitioning = True
                ) t
            ON
                effective_timestamp = t.timestamp
                AND c.service_id = t.service_id
            INNER JOIN
                read_parquet(
                    '{s3_prefix}/STOP_TIMES/timestamp=*/*.parquet',
                    hive_partitioning = True
                ) st
            ON
                effective_timestamp = st.timestamp
                AND t.trip_id = st.trip_id
            INNER JOIN
                read_parquet(
                    '{s3_prefix}/STOPS/timestamp=*/*.parquet',
                    hive_partitioning = True
                ) s
            ON
                effective_timestamp = s.timestamp
                AND st.stop_id = s.stop_id
            ORDER BY service_date, route_id, t.trip_id, departure_dt
            """
        )
        pl.log_complete()
        return True
    except duckdb.Error as e:
        pl.log_failure(e)
        return False


def add_views_to_local_schema(
    connection: duckdb.DuckDBPyConnection,
    views: List[rf.S3Location],
    partition_strategy: str = "",
    schema: str | None = None,
) -> List[str]:
    """Add views of remote Parquet files to duckdb schema."""
    built_views: List[str] = []
    for item in views:
        view_name = (schema + ".") if schema else "" + os.path.splitext(os.path.basename(item.prefix))[0]
        result = build_view(connection, view_name, item, partition_strategy)
        if result:
            built_views.append(view_name)

    if not built_views:
        raise EmptyDataStructureException

    return built_views


def pipeline(  # pylint: disable=dangerous-default-value
    views: list[tuple[str, list[rf.S3Location], str]] = VIEWS,
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
        create_schemas(con, [v[0] for v in views])
        for v in views:
            add_views_to_local_schema(con, v[1], v[2], v[0])
        register_read_ymd(con)
        register_effective_gtfs_timestamps(con)
        register_gtfs_schedule_view(con)

    if remote_location:
        pl.add_metadata(remote_location=remote_location.s3_uri)
        upload_file(local_location, remote_location.s3_uri)

    pl.log_complete()
