import os
import shutil
from typing import List

from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceRecent, HyperBusPerformanceAll
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob, days_ago
from datetime import date
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_ARCHIVE,
    S3Location,
    bus_events,
)
from lamp_py.tableau.jobs.bus_performance import bus_schema
from lamp_py.tableau.conversions.convert_bus_performance_data import apply_bus_analysis_conversions


from lamp_py.tableau.jobs.lamp_jobs import (
    LAMP_API_PROJECT,
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperGtfsRtVehiclePositionsHeavyRail,
    HyperGtfsRtTripUpdatesHeavyRail,
    HyperGtfsRtVehiclePositionsAllLightRail,
    HyperDevGreenGtfsRtVehiclePositions,
    HyperDevGreenGtfsRtTripUpdates,
    HyperBusOperatorMappingRecent,
    HyperBusOperatorMappingAll,
    # HyperRtRailSubway,
    # HyperRtRailCommuter,
)

TestHyperGtfsRtVehiclePositions = HyperGtfsRtVehiclePositions
TestHyperGtfsRtTripUpdates = HyperGtfsRtTripUpdates
TestHyperGtfsRtVehiclePositionsHeavyRail = HyperGtfsRtVehiclePositionsHeavyRail
TestHyperGtfsRtTripUpdatesHeavyRail = HyperGtfsRtTripUpdatesHeavyRail
TestHyperGtfsRtVehiclePositionsAllLightRail = HyperGtfsRtVehiclePositionsAllLightRail
TestHyperDevGreenGtfsRtVehiclePositions = HyperDevGreenGtfsRtVehiclePositions
TestHyperDevGreenGtfsRtTripUpdates = HyperDevGreenGtfsRtTripUpdates
TestHyperBusOperatorMappingRecent = HyperBusOperatorMappingRecent
TestHyperBusOperatorMappingAll = HyperBusOperatorMappingAll
# TestHyperRtRailSubway = HyperRtRailSubway
# TestHyperRtRailCommuter = HyperRtRailCommuter
TestHyperBusPerformanceRecent = HyperBusPerformanceRecent
TestHyperBusPerformanceAll = HyperBusPerformanceAll


TestHyperGtfsRtVehiclePositions.start_date = days_ago(1)
TestHyperGtfsRtTripUpdates.start_date = days_ago(1)
TestHyperGtfsRtVehiclePositionsHeavyRail.start_date = days_ago(1)
TestHyperGtfsRtTripUpdatesHeavyRail.start_date = days_ago(1)
TestHyperGtfsRtVehiclePositionsAllLightRail.start_date = days_ago(1)
TestHyperDevGreenGtfsRtVehiclePositions.start_date = days_ago(1)
TestHyperDevGreenGtfsRtTripUpdates.start_date = days_ago(1)
TestHyperBusOperatorMappingRecent.start_date = days_ago(1)
TestHyperBusOperatorMappingAll.start_date = days_ago(1)
# TestHyperRtRailSubway.start_date=days_ago(1)
# TestHyperRtRailCommuter.start_date=days_ago(1)

TestHyperBus = FilteredHyperJob(
    remote_input_location=bus_events,
    remote_output_location=S3Location(
        bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "bus_rating_datasets", "year=2025", "Fall.parquet"), version="1.0"
    ),
    start_date=date(2025, 12, 12),
    end_date=date(2025, 12, 13),
    processed_schema=bus_schema,
    dataframe_filter=apply_bus_analysis_conversions,
    parquet_filter=None,
    tableau_project_name=LAMP_API_PROJECT,
    partition_template="{yy}{mm:02d}{dd:02d}.parquet",
)


def start_hyper() -> None:
    """Run all HyperFile Update Jobs"""

    hyper_jobs: List[HyperJob] = [
        TestHyperGtfsRtVehiclePositions,
        TestHyperGtfsRtTripUpdates,
        TestHyperGtfsRtVehiclePositionsHeavyRail,
        TestHyperGtfsRtTripUpdatesHeavyRail,
        TestHyperGtfsRtVehiclePositionsAllLightRail,
        TestHyperDevGreenGtfsRtVehiclePositions,
        TestHyperDevGreenGtfsRtTripUpdates,
        TestHyperBusOperatorMappingRecent,
        TestHyperBusOperatorMappingAll,
        TestHyperBus,
        # TestHyperRtRailSubway,
        # TestHyperRtRailCommuter,
    ]

    local_parquet = True
    run_pq_remote = False
    local_hyper = False
    run_hyper_remote = False

    if local_parquet:
        for job in hyper_jobs:
            # try:
            outs = job.create_parquet(None)
            shutil.copy(job.local_parquet_path, job.local_hyper_path.replace(".hyper", ".parquet"))
            # except Exception as e:
            #     print(f"{job.remote_parquet_path} parquet/local unable to generate - {e}")

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
