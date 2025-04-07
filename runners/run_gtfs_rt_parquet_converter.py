from typing import List

from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates, convert_gtfs_rt_vehicle_position
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.gtfs_rt_preprocessor import FilteredHyperJob
from lamp_py.tableau.pipeline import (
    start_devgreen_gtfs_rt_parquet_updates,
    start_gtfs_rt_parquet_updates,
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperDevGreenGtfsRtVehiclePositions,
    HyperDevGreenGtfsRtTripUpdates,
    rt_vehicle_positions,
    rt_trip_updates,
    tableau_rt_vehicle_positions_lightrail_30day,
)


from lamp_py.utils.filter_bank import FilterBank_RtTripUpdates, FilterBank_RtVehiclePositions


# don't run this in pytest - environment variables in pyproject.toml point to local SPRINGBOARD/ARCHIVE
# need the .env values to run
def start_devgreen_gtfs_rt_parquet_updates_local() -> None:
    """Run all gtfs_rt Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [HyperDevGreenGtfsRtVehiclePositions, HyperDevGreenGtfsRtTripUpdates]

    for job in parquet_update_jobs:
        # breakpoint()
        job.run_parquet(None)
        # outs = job.create_local_hyper()
        # print(outs)


def start_gtfs_rt_parquet_updates_local() -> None:
    """Run all gtfs_rt Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [HyperGtfsRtVehiclePositions, HyperGtfsRtTripUpdates]

    for job in parquet_update_jobs:
        # breakpoint()
        job.run_parquet(None)
        # outs = job.create_local_hyper()
        # print(outs)


if __name__ == "__main__":
    start_gtfs_rt_parquet_updates_local()
    start_devgreen_gtfs_rt_parquet_updates_local()
