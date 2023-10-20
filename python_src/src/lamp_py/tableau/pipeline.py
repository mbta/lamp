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

HYPER_JOBS = (
    HyperRtRail(),
    HyperServiceIdByRoute(),
    HyperStaticCalendar(),
    HyperStaticCalendarDates(),
    HyperStaticFeedInfo(),
    HyperStaticRoutes(),
    HyperStaticStops(),
    HyperStaticStopTimes(),
    HyperStaticTrips(),
)


def start_hyper_updates() -> None:
    """Run all HyperFile Update Jobs"""
    for job in HYPER_JOBS:
        job.run_hyper()


def start_parquet_updates() -> None:
    """Run all Parquet Update jobs"""
    for job in HYPER_JOBS:
        job.run_parquet()
