import os
from typing import List

import duckdb
from lamp_py.runtime_utils import remote_files as rf
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import upload_file

HIVE_VIEWS = {
    "/*/*/*/*.parquet": [
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
    "/*.parquet": [
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
        rf.public_alerts_file,
        rf.tableau_bus_all,
        rf.tableau_bus_operator_mapping_all,
        rf.tableau_rt_vehicle_positions_lightrail_60_day,
        rf.tableau_rt_trip_updates_heavyrail_30_day,
        rf.tableau_devgreen_rt_trip_updates_lightrail_60_day,
        rf.tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    ],
}


def authenticate(connection: duckdb.DuckDBPyConnection) -> bool:
    "Register IAM credentials with duckdb."
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
) -> str:
    "Create view using data location according to partitions."
    pl = ProcessLogger("build_view")

    view_target = f"{data_location.s3_uri}{partition_strategy}"
    pl.add_metadata(view_name=view_name, view_target=view_target)

    connection.from_parquet(view_target, hive_partitioning=True).create_view(view_name)
    pl.log_complete()

    return view_name


def add_views_to_local_metastore(
    connection: duckdb.DuckDBPyConnection, views: dict[str, List[rf.S3Location]]
) -> List[str]:
    "Add views of remote Parquet files to duckdb database."

    with connection as con:
        built_views: List[str] = []
        for k in views.keys():
            for item in views[k]:
                built_views.append(build_view(con, os.path.splitext(os.path.basename(item.prefix))[0], item, k))

    return built_views


def pipeline(  # pylint: disable=dangerous-default-value
    views: dict[str, List[rf.S3Location]] = HIVE_VIEWS,
    local_location: str = "/tmp/lamp.db",
    remote_location: rf.S3Location | None = rf.lightswitch,
) -> None:
    "Create duckdb metastore and upload to specified location."
    pl = ProcessLogger("lightswitch.pipeline", local_location=local_location)
    pl.log_start()

    os.environ["SERVICE_NAME"] = "lightswitch"

    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "PUBLIC_ARCHIVE_BUCKET",
            "INCOMING_BUCKET",
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
        ],
    )

    with duckdb.connect(local_location) as con:
        auth = authenticate(con)
        pl.add_metadata(authenticated=auth)
        add_views_to_local_metastore(con, views)

    if remote_location:
        pl.add_metadata(remote_location=remote_location.s3_uri)
        upload_file(local_location, remote_location.s3_uri)

    pl.log_complete()
