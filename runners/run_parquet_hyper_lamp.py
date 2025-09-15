import os
import shutil
from typing import List

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates, convert_gtfs_rt_vehicle_position
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll, HyperBusPerformanceRecent
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.pipeline import (
    start_bus_parquet_updates,
)
from lamp_py.tableau.jobs.lamp_jobs import (
    GTFS_RT_TABLEAU_PROJECT,
    HyperDevGreenGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
)
from lamp_py.utils.filter_bank import FilterBankRtTripUpdates, FilterBankRtVehiclePositions

from lamp_py.runtime_utils.remote_files import (
    # springboard_rt_vehicle_positions,
    springboard_devgreen_rt_vehicle_positions,
    springboard_rt_trip_updates,  # main feed, all lines, unique records
    springboard_devgreen_lrtp_trip_updates,  # dev green feed, green line only, all records
    # springboard_lrtp_trip_updates,  # main feed, green line only, all records
    # tableau_rt_vehicle_positions_lightrail_60_day,
    tableau_rt_trip_updates_lightrail_60_day,
    # tableau_rt_vehicle_positions_heavyrail_30_day,
    tableau_rt_trip_updates_heavyrail_30_day,
    tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
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
    rollup_num_days=3,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.heavyrail,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.heavy_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

TestHyperDevGreenGtfsRtVehiclePositions = FilteredHyperJob(
    remote_input_location=springboard_devgreen_rt_vehicle_positions,
    remote_output_location=tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    rollup_num_days=3,
    processed_schema=convert_gtfs_rt_vehicle_position.schema(),
    dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
    parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)


def start_hyper() -> None:
    """Run all HyperFile Update Jobs"""

    hyper_jobs: List[HyperJob] = [
        # HyperBusPerformanceAll(),
        # HyperBusPerformanceRecent(),
        # HyperDevGreenGtfsRtTripUpdates,
        # HyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdatesHeavyRail,
        # TestHyperGtfsRtTripUpdates,
        TestHyperDevGreenGtfsRtTripUpdates,
        TestHyperDevGreenGtfsRtVehiclePositions,
    ]

    local_parquet = True
    run_pq_remote = False
    local_hyper = True
    run_hyper_remote = False

    if local_parquet:
        for job in hyper_jobs:
            try:
                outs = job.create_parquet(None)
                shutil.copy(job.local_parquet_path, job.local_hyper_path.replace(".hyper", ".parquet"))
            except Exception as e:
                print(f"{job.remote_parquet_path} parquet/local unable to generate - {e}")

    if run_pq_remote:
        for job in hyper_jobs:
            try:
                outs = job.run_parquet(None)
            except Exception as e:
                print(f"{job.remote_parquet_path} parquet/upload unable to generate - {e}")

    if local_hyper:
        for job in hyper_jobs:
            try:
                shutil.copy(job.local_hyper_path.replace(".hyper", ".parquet"), job.local_parquet_path)
                outs = job.create_local_hyper(use_local=True)
            except Exception as e:
                print(f"{job.remote_parquet_path} hyper/local unable to generate - {e}")

    if run_hyper_remote:
        for job in hyper_jobs:
            try:
                outs = job.run_hyper()
            except Exception as e:
                print(f"{job.remote_parquet_path} hyper/upload unable to generate - {e}")


if __name__ == "__main__":

    start_hyper()
