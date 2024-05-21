import os
from typing import List

from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.tableau.hyper import RdsHyperJob, ParquetHyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.performance_manager.alerts import AlertsS3Info
from lamp_py.tableau.jobs.rt_rail import HyperRtRail
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
from lamp_py.aws.ecs import check_for_parallel_tasks


def rds_hyper_jobs() -> List[RdsHyperJob]:
    """Create Hyper Jobs that Publish from RDS"""
    return [
        HyperRtRail(),
        HyperServiceIdByRoute(),
        HyperStaticCalendar(),
        HyperStaticCalendarDates(),
        HyperStaticFeedInfo(),
        HyperStaticRoutes(),
        HyperStaticStops(),
        HyperStaticStopTimes(),
        HyperStaticTrips(),
    ]


def parquet_hyper_jobs() -> List[ParquetHyperJob]:
    """Create Hyper Jobs that Publish from Parquet Files"""
    return [
        # Alerts Hyper Job
        ParquetHyperJob(
            hyper_file_name="LAMP_ALERTS.hyper",
            remote_parquet_path=AlertsS3Info.s3_path,
            lamp_version=AlertsS3Info.file_version,
        )
    ]


def start_hyper_updates() -> None:
    """Run all HyperFile Update Jobs"""
    # configure the environment
    os.environ["SERVICE_NAME"] = "tableau_hyper_update"

    validate_environment(
        required_variables=[
            "TABLEAU_USER",
            "TABLEAU_PASSWORD",
            "TABLEAU_SERVER",
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

    # rds and parquet jobs all get published
    hyper_jobs = rds_hyper_jobs() + parquet_hyper_jobs()
    for job in hyper_jobs:
        job.run_hyper()


def start_parquet_updates(db_manager: DatabaseManager) -> None:
    """Run all Parquet Update jobs"""
    for job in rds_hyper_jobs():
        job.run_parquet(db_manager)
