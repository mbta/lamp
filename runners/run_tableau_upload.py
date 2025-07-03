import os
from typing import List

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll, HyperBusPerformanceRecent
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

from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns
from lamp_py.tableau.jobs.glides import HyperGlidesTripUpdates
from lamp_py.tableau.pipeline import (
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperGtfsRtVehiclePositionsHeavyRail,
    HyperGtfsRtTripUpdatesHeavyRail,
    HyperGtfsRtVehiclePositionsAllLightRail,
    HyperDevGreenGtfsRtVehiclePositions,
    HyperDevGreenGtfsRtTripUpdates,
)


def start_bus_hyper() -> None:
    """Run all HyperFile Update Jobs"""
    # configure the environment
    os.environ["SERVICE_NAME"] = "tableau_hyper_update_hh"

    validate_environment(
        required_variables=[
            "TABLEAU_USER",
            "TABLEAU_PASSWORD",
            "TABLEAU_SERVER",
            "TABLEAU_PROJECT",
            "PUBLIC_ARCHIVE_BUCKET",
            # "ECS_CLUSTER",
            # "ECS_TASK_GROUP",
        ],
        private_variables=[
            "TABLEAU_PASSWORD",
        ],
    )

    # make sure only one publisher runs at a time
    check_for_parallel_tasks()

    hyper_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),
        HyperGlidesOperatorSignIns(),
        # HyperRtRail(), # skip this one its big
        HyperServiceIdByRoute(),
        HyperStaticCalendar(),
        HyperStaticCalendarDates(),
        HyperStaticFeedInfo(),
        HyperStaticRoutes(),
        HyperStaticStops(),
        HyperStaticStopTimes(),
        HyperStaticTrips(),
        HyperRtAlerts(),
        # HyperBusPerformanceAll(), # skip this one it's big
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
        try:
            outs = job.run_hyper()
        except Exception as e:
            print(e)


if __name__ == "__main__":

    start_bus_hyper()
