import os
from typing import List

from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates, convert_gtfs_rt_vehicle_position
from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.tableau.jobs.rt_rail import HyperRtRail
from lamp_py.tableau.jobs.rt_alerts import HyperRtAlerts
from lamp_py.tableau.jobs.gtfs_rail import (
    HyperServiceIdByRoute,
    HyperStaticCalendar,
    HyperStaticCalendarDates,
    HyperStaticFeedInfo,
    HyperStaticRoutes,
    HyperStaticStops,
    HyperStaticStopTimes,
    HyperStaticTrips,
)
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceRecent
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns
from lamp_py.tableau.jobs.glides import HyperGlidesTripUpdates
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.utils.filter_bank import FilterBankRtTripUpdates, FilterBankRtVehiclePositions
from lamp_py.aws.ecs import check_for_parallel_tasks

from lamp_py.runtime_utils.remote_files import (
    springboard_rt_vehicle_positions,
    springboard_rt_trip_updates,
    springboard_devgreen_rt_vehicle_positions,
    springboard_devgreen_rt_trip_updates,
    tableau_rt_vehicle_positions_lightrail_60_day,
    tableau_rt_trip_updates_lightrail_60_day,
    tableau_rt_vehicle_positions_heavyrail_30_day,
    tableau_rt_trip_updates_heavyrail_30_day,
    tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    tableau_devgreen_rt_trip_updates_lightrail_60_day,
)

GTFS_RT_TABLEAU_PROJECT = "GTFS-RT"

HyperGtfsRtVehiclePositions = FilteredHyperJob(
    remote_input_location=springboard_rt_vehicle_positions,
    remote_output_location=tableau_rt_vehicle_positions_lightrail_60_day,
    rollup_num_days=60,
    processed_schema=convert_gtfs_rt_vehicle_position.schema(),
    dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
    parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperGtfsRtTripUpdates = FilteredHyperJob(
    remote_input_location=springboard_rt_trip_updates,
    remote_output_location=tableau_rt_trip_updates_lightrail_60_day,
    rollup_num_days=60,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_prod,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperGtfsRtVehiclePositionsHeavyRail = FilteredHyperJob(
    remote_input_location=springboard_rt_vehicle_positions,
    remote_output_location=tableau_rt_vehicle_positions_heavyrail_30_day,
    rollup_num_days=30,
    processed_schema=convert_gtfs_rt_vehicle_position.schema(),
    dataframe_filter=convert_gtfs_rt_vehicle_position.heavyrail,
    parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.heavy_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperGtfsRtTripUpdatesHeavyRail = FilteredHyperJob(
    remote_input_location=springboard_rt_trip_updates,
    remote_output_location=tableau_rt_trip_updates_heavyrail_30_day,
    rollup_num_days=30,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.heavyrail,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.heavy_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperDevGreenGtfsRtVehiclePositions = FilteredHyperJob(
    remote_input_location=springboard_devgreen_rt_vehicle_positions,
    remote_output_location=tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    rollup_num_days=60,
    processed_schema=convert_gtfs_rt_vehicle_position.schema(),
    dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
    parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperDevGreenGtfsRtTripUpdates = FilteredHyperJob(
    remote_input_location=springboard_devgreen_rt_trip_updates,
    remote_output_location=tableau_devgreen_rt_trip_updates_lightrail_60_day,
    rollup_num_days=60,
    processed_schema=convert_gtfs_rt_trip_updates.schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_devgreen,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)


def start_hyper_updates() -> None:
    """Run all HyperFile Update Jobs"""
    # configure the environment
    os.environ["SERVICE_NAME"] = "tableau_hyper_update"

    validate_environment(
        required_variables=[
            "TABLEAU_USER",
            "TABLEAU_PASSWORD",
            "TABLEAU_SERVER",
            "TABLEAU_PROJECT",
            "PUBLIC_ARCHIVE_BUCKET",
            "ECS_CLUSTER",
            "ECS_TASK_GROUP",
        ],
        private_variables=[
            "TABLEAU_PASSWORD",
        ],
    )

    # make sure only one publisher runs at a time
    check_for_parallel_tasks()

    hyper_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),  # glides have operational usage, run these first to ensure timely report gen
        HyperGlidesOperatorSignIns(),  # glides have operational usage, run these first to ensure timely report gen
        HyperRtRail(),
        HyperServiceIdByRoute(),
        HyperStaticCalendar(),
        HyperStaticCalendarDates(),
        HyperStaticFeedInfo(),
        HyperStaticRoutes(),
        HyperStaticStops(),
        HyperStaticStopTimes(),
        HyperStaticTrips(),
        HyperRtAlerts(),
        HyperBusPerformanceAll(),
        HyperBusPerformanceRecent(),
        HyperGtfsRtVehiclePositions,
        HyperGtfsRtTripUpdates,
        HyperDevGreenGtfsRtVehiclePositions,
        HyperDevGreenGtfsRtTripUpdates,
        HyperGtfsRtVehiclePositionsHeavyRail,
        HyperGtfsRtTripUpdatesHeavyRail,
    ]

    for job in hyper_jobs:
        job.run_hyper()


def start_parquet_updates(db_manager: DatabaseManager) -> None:
    """Run all Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),  # glides have operational usage, run these first to ensure timely report gen
        HyperGlidesOperatorSignIns(),  # glides have operational usage, run these first to ensure timely report gen
        HyperRtRail(),
        HyperServiceIdByRoute(),
        HyperStaticCalendar(),
        HyperStaticCalendarDates(),
        HyperStaticFeedInfo(),
        HyperStaticRoutes(),
        HyperStaticStops(),
        HyperStaticStopTimes(),
        HyperStaticTrips(),
        HyperGtfsRtVehiclePositions,
        HyperGtfsRtTripUpdates,
        HyperDevGreenGtfsRtVehiclePositions,
        HyperDevGreenGtfsRtTripUpdates,
        HyperGtfsRtVehiclePositionsHeavyRail,
        HyperGtfsRtTripUpdatesHeavyRail,
    ]

    for job in parquet_update_jobs:
        job.run_parquet(db_manager)


def start_bus_parquet_updates() -> None:
    """Run all Bus Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [
        HyperBusPerformanceRecent(),
        HyperBusPerformanceAll(),
    ]

    for job in parquet_update_jobs:
        job.run_parquet(None)
