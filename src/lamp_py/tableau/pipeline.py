import os
from typing import List

from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.tableau.jobs.lamp_jobs import (
    HyperDevGreenGtfsRtTripUpdates,
    HyperDevGreenGtfsRtVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtVehiclePositionsAllLightRail,
    HyperGtfsRtVehiclePositionsHeavyRail,
)
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
from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.tableau.jobs.spare_jobs import HyperSpareVehicles


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
        HyperGtfsRtVehiclePositionsAllLightRail,
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
        HyperBusPerformanceAll(),
    ]

    for job in parquet_update_jobs:
        job.run_parquet(None)


def start_spare_updates() -> None:
    """
    Run all Spare Parquet Update jobs
    called from Tableau Publisher
    """

    jobs: List[HyperJob] = [
        HyperSpareVehicles,
    ]

    for job in jobs:
        job.run_parquet_hyper_one_shot(None)
