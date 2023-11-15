import os
import gc
from typing import List

from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.tableau.hyper import HyperJob
from lamp_py.postgres.postgres_utils import DatabaseManager
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


def create_hyper_jobs() -> List[HyperJob]:
    """Create Hyper Jobs to Run"""
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
        ],
        private_variables=[
            "TABLEAU_PASSWORD",
        ],
    )
    for job in create_hyper_jobs():
        job.run_hyper()


def start_parquet_updates(db_manager: DatabaseManager) -> None:
    """Run all Parquet Update jobs"""
    for job in create_hyper_jobs():
        job.run_parquet(db_manager)

    # ECS Memory Utilization appeared to stay elevated after initial calling
    # of `run_parquet` to create all parquet files, this may resolve that...
    gc.collect()
