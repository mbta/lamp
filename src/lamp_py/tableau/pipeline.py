import os
from typing import List

from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.tableau.jobs.lamp_jobs import (
    HyperBusOperatorMappingAll,
    HyperBusOperatorMappingRecent,
    HyperDevGreenGtfsRtLightRailTripUpdates,
    HyperDevGreenGtfsRtLightRailVehiclePositions,
    HyperDevGreenGtfsRtHeavyRailTripUpdates,
    HyperDevGreenGtfsRtHeavyRailVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtVehiclePositionsAllLightRail,
    HyperGtfsRtVehiclePositionsHeavyRail,
    HyperRtRailSubway,
    HyperRtRailCommuter,
    HyperBusFall2025,
    HyperBusOperatorFall2025,
)
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
from lamp_py.aws.ecs import check_for_parallel_tasks


def start_hyper_updates() -> None:
    """Run all HyperFile Update Jobs
    called on cron defined in terraform-aws-mbta-lamp/main.tf:525
    """
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
        optional_variables=["TABLEAU_TOKEN_NAME"],
        private_variables=[
            "TABLEAU_PASSWORD",
        ],
    )

    # make sure only one publisher runs at a time
    check_for_parallel_tasks()

    # parquet and hyper all in one
    start_spare_updates()

    #
    hyper_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),  # glides have operational usage, run these first to ensure timely report gen
        HyperGlidesOperatorSignIns(),  # glides have operational usage, run these first to ensure timely report gen
        HyperRtRailSubway,
        HyperRtRailCommuter,
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
        HyperBusOperatorMappingRecent,
        HyperBusOperatorMappingAll,
        HyperGtfsRtVehiclePositions,
        HyperGtfsRtTripUpdates,
        HyperDevGreenGtfsRtLightRailVehiclePositions,
        HyperDevGreenGtfsRtLightRailTripUpdates,
        HyperDevGreenGtfsRtHeavyRailVehiclePositions,
        HyperDevGreenGtfsRtHeavyRailTripUpdates,
        HyperGtfsRtVehiclePositionsHeavyRail,
        HyperGtfsRtTripUpdatesHeavyRail,
        HyperGtfsRtVehiclePositionsAllLightRail,
        HyperBusFall2025,
        HyperBusOperatorFall2025,
    ]

    for job in hyper_jobs:
        job.run_hyper()


def start_parquet_updates(db_manager: DatabaseManager) -> None:
    """Run all Parquet Update jobs
    called from performance manager
    """

    parquet_update_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),  # glides have operational usage, run these first to ensure timely report gen
        HyperGlidesOperatorSignIns(),  # glides have operational usage, run these first to ensure timely report gen
        HyperRtRailSubway,
        HyperRtRailCommuter,
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
        HyperDevGreenGtfsRtLightRailVehiclePositions,
        HyperDevGreenGtfsRtLightRailTripUpdates,
        HyperDevGreenGtfsRtHeavyRailVehiclePositions,
        HyperDevGreenGtfsRtHeavyRailTripUpdates,
        HyperGtfsRtVehiclePositionsHeavyRail,
        HyperGtfsRtTripUpdatesHeavyRail,
        HyperGtfsRtVehiclePositionsAllLightRail,
    ]

    for job in parquet_update_jobs:
        job.run_parquet(db_manager)


def start_bus_parquet_updates() -> None:
    """Run all Bus Parquet Update jobs
    called from bus performance manager
    """

    parquet_update_jobs: List[HyperJob] = [
        HyperBusPerformanceRecent(),
        HyperBusOperatorMappingRecent,
        HyperBusPerformanceAll(),
        HyperBusOperatorMappingAll,
        HyperBusFall2025,
        HyperBusOperatorFall2025,
    ]

    for job in parquet_update_jobs:
        job.run_parquet(None)


def start_spare_updates() -> None:
    """
    Run all Spare Parquet Update jobs
    called from Tableau Publisher
    """
    # move this expensive import into here and only call when start_spare_updates() is called
    # this import checks the whitelist of spare jobs, and dynamically generates
    # input and output schemas based on what is in the spare s3 bucket

    from lamp_py.tableau.jobs.spare_jobs import spare_job_list  # pylint: disable=C0415

    for job in spare_job_list:
        job.run_parquet_hyper_combined_job(None)
