import os
from typing import List

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll, HyperBusPerformanceRecent
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.pipeline import (
    GTFS_RT_TABLEAU_PROJECT,
    start_bus_parquet_updates,
    HyperDevGreenGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
)
from lamp_py.utils.filter_bank import FilterBankRtTripUpdates

from lamp_py.runtime_utils.remote_files import (
    # springboard_rt_vehicle_positions,
    # springboard_devgreen_rt_vehicle_positions,
    springboard_rt_trip_updates,  # main feed, all lines, unique records
    springboard_devgreen_lrtp_trip_updates,  # dev green feed, green line only, all records
    # springboard_lrtp_trip_updates,  # main feed, green line only, all records
    # tableau_rt_vehicle_positions_lightrail_60_day,
    tableau_rt_trip_updates_lightrail_60_day,
    # tableau_rt_vehicle_positions_heavyrail_30_day,
    tableau_rt_trip_updates_heavyrail_30_day,
    # tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    tableau_devgreen_rt_trip_updates_lightrail_60_day,
    # tableau_rt_vehicle_positions_all_light_rail_7_day,
)

TestHyperDevGreenGtfsRtTripUpdates = FilteredHyperJob(
    remote_input_location=springboard_devgreen_lrtp_trip_updates,
    remote_output_location=tableau_devgreen_rt_trip_updates_lightrail_60_day,
    rollup_num_days=2,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_devgreen,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

TestHyperGtfsRtTripUpdates = FilteredHyperJob(
    remote_input_location=springboard_rt_trip_updates,
    remote_output_location=tableau_rt_trip_updates_lightrail_60_day,
    rollup_num_days=2,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_devgreen,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

TestHyperGtfsRtTripUpdatesHeavyRail = FilteredHyperJob(
    remote_input_location=springboard_rt_trip_updates,
    remote_output_location=tableau_rt_trip_updates_heavyrail_30_day,
    rollup_num_days=30,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.heavyrail,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.heavy_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)


def start_hyper() -> None:
    """Run all HyperFile Update Jobs"""
    # configure the environment
    # os.environ["SERVICE_NAME"] = "tableau_hyper_update"

    # validate_environment(
    #     required_variables=[
    #         "TABLEAU_USER",
    #         "TABLEAU_PASSWORD",
    #         "TABLEAU_SERVER",
    #         "TABLEAU_PROJECT",
    #         "PUBLIC_ARCHIVE_BUCKET",
    #         "ECS_CLUSTER",
    #         "ECS_TASK_GROUP",
    #     ],
    #     private_variables=[
    #         "TABLEAU_PASSWORD",
    #     ],
    # )

    # make sure only one publisher runs at a time
    # check_for_parallel_tasks()

    hyper_jobs: List[HyperJob] = [
        # HyperBusPerformanceAll(),
        # HyperBusPerformanceRecent(),
        # HyperDevGreenGtfsRtTripUpdates,
        # HyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdates,
        TestHyperDevGreenGtfsRtTripUpdates,
    ]
    # breakpoint()

    run_pq = True
    if run_pq:
        for job in hyper_jobs:
            outs = job.create_parquet(None)

    hyper = False
    if hyper:
        for job in hyper_jobs:
            outs = job.create_local_hyper()


if __name__ == "__main__":
    start_hyper()
    # start_bus_parquet_updates()
