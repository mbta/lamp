import os
from typing import List

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll, HyperBusPerformanceRecent
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.spare_jobs import HyperSpareVehicles, spare_job_list
from lamp_py.tableau.jobs.spare_jobs import SPARE_TABLEAU_PROJECT
from lamp_py.tableau.pipeline import (
    start_bus_parquet_updates,
)
from lamp_py.tableau.jobs.lamp_jobs import (
    GTFS_RT_TABLEAU_PROJECT,
    HyperDevGreenGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
)
from lamp_py.tableau.spare.default_converter import default_converter, default_converter_from_s3
from lamp_py.utils.filter_bank import FilterBankRtTripUpdates
from lamp_py.tableau.spare.autogen_01_schema_printer import springboard_spare_cases_with_history, tableau_spare_cases_with_history
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

def start_spare_single() -> None:

    job = FilteredHyperJob(
        remote_input_location=springboard_spare_cases_with_history,
        remote_output_location=tableau_spare_cases_with_history,
        rollup_num_days=None,
        processed_schema=default_converter_from_s3(springboard_spare_cases_with_history),
        parquet_preprocess=None,
        dataframe_filter=default_converter,
        parquet_filter=None,
        tableau_project_name=SPARE_TABLEAU_PROJECT,
    )
    outs = job.run_parquet(None)
    outs2 = job.create_local_hyper()    

def start_spare() -> None:
    """Run all HyperFile Update Jobs"""

    # hyper_jobs: List[HyperJob] = [
    #     HyperSpareVehicles,
    # ]
    # for job in hyper_jobs:
    #     outs = job.run_parquet_hyper_combined_job()


    # spare_job_list = []
    for job in spare_job_list:
        # outs = job.run_parquet_hyper_combined_job()
        try:
            outs = job.run_parquet(None)
            outs2 = job.create_local_hyper()
        except Exception as e:
            print(f"{job.remote_parquet_path} unable to generate - {e}")

def start_hyper() -> None:
    """Run all HyperFile Update Jobs"""

    hyper_jobs: List[HyperJob] = [
        # HyperBusPerformanceAll(),
        HyperBusPerformanceRecent(),
        # HyperDevGreenGtfsRtTripUpdates,
        # HyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdates,
        # TestHyperDevGreenGtfsRtTripUpdates,
    ]
    # breakpoint()

    run_create_pq = False
    if run_create_pq:
        for job in hyper_jobs:
            outs = job.create_parquet(None)

    run_pq = True
    if run_pq:
        for job in hyper_jobs:
            outs = job.run_parquet(None)

    local_hyper = False
    if local_hyper:
        for job in hyper_jobs:
            outs = job.create_local_hyper()

    hyper = True
    if hyper:
        for job in hyper_jobs:
            outs = job.run_hyper()


if __name__ == "__main__":
    start_spare()
    # start_spare_single()
    # start_hyper()
