import os
import shutil
from typing import List

from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceRecent, HyperBusPerformanceAll
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from datetime import date

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
    Prod_VehiclePositions_LightRailTerminals_60Day,
    Prod_TripUpdates_LightRailTerminals_60Day,
    Prod_VehiclePositions_HeavyRailTerminals_30Day,
    Prod_TripUpdates_HeavyRailTerminals_30Day,
    Prod_VehiclePositions_LightRail_7Day,
    DevGreen_VehiclePositions_LightRailTerminals_60Day,
    DevGreen_TripUpdates_LightRailTerminals_60Day,
    DevGreen_VehiclePositions_HeavyRailTerminals_60Day,
    DevGreen_TripUpdates_HeavyRailTerminals_60Day,
    Prod_BusOperatorMapping_Recent,
    Prod_BusOperatorMapping_LongTerm,
    Prod_BusMetrics_Fall2025Rating,
    Prod_BusOperatorMapping_Fall2025Rating,
    Prod_RailMetrics_Subway_LongTerm,
    Prod_RailMetrics_CommuterRail_LongTerm,
)

TestProd_VehiclePositions_LightRailTerminals_60Day = Prod_VehiclePositions_LightRailTerminals_60Day
TestProd_TripUpdates_LightRailTerminals_60Day = Prod_TripUpdates_LightRailTerminals_60Day
TestProd_VehiclePositions_HeavyRailTerminals_30Day = Prod_VehiclePositions_HeavyRailTerminals_30Day
TestProd_TripUpdates_HeavyRailTerminals_30Day = Prod_TripUpdates_HeavyRailTerminals_30Day
TestProd_VehiclePositions_LightRail_7Day = Prod_VehiclePositions_LightRail_7Day
TestDevGreen_VehiclePositions_LightRailTerminals_60Day = DevGreen_VehiclePositions_LightRailTerminals_60Day
TestDevGreen_TripUpdates_LightRailTerminals_60Day = DevGreen_TripUpdates_LightRailTerminals_60Day
TestDevGreen_VehiclePositions_HeavyRailTerminals_60Day = DevGreen_VehiclePositions_HeavyRailTerminals_60Day
TestDevGreen_TripUpdates_HeavyRailTerminals_60Day = DevGreen_TripUpdates_HeavyRailTerminals_60Day
TestProd_BusOperatorMapping_Recent = Prod_BusOperatorMapping_Recent
TestProd_BusOperatorMapping_LongTerm = Prod_BusOperatorMapping_LongTerm
TestProd_BusMetrics_Fall2025Rating = Prod_BusMetrics_Fall2025Rating
TestProd_BusOperatorMapping_Fall2025Rating = Prod_BusOperatorMapping_Fall2025Rating
TestProd_RailMetrics_Subway_LongTerm = Prod_RailMetrics_Subway_LongTerm
TestProd_RailMetrics_CommuterRail_LongTerm = Prod_RailMetrics_CommuterRail_LongTerm
TestHyperBusPerformanceRecent = HyperBusPerformanceRecent
TestHyperBusPerformanceAll = HyperBusPerformanceAll

yesterday = 1
TestProd_VehiclePositions_LightRailTerminals_60Day.num_days_ago = yesterday
TestProd_TripUpdates_LightRailTerminals_60Day.num_days_ago = yesterday
TestProd_VehiclePositions_HeavyRailTerminals_30Day.num_days_ago = yesterday
TestProd_TripUpdates_HeavyRailTerminals_30Day.num_days_ago = yesterday
TestProd_VehiclePositions_LightRail_7Day.num_days_ago = yesterday
TestDevGreen_VehiclePositions_LightRailTerminals_60Day.num_days_ago = yesterday
TestDevGreen_TripUpdates_LightRailTerminals_60Day.num_days_ago = yesterday
TestDevGreen_VehiclePositions_HeavyRailTerminals_60Day.num_days_ago = yesterday
TestDevGreen_TripUpdates_HeavyRailTerminals_60Day.num_days_ago = yesterday
TestProd_BusOperatorMapping_Recent.num_days_ago = yesterday
TestProd_BusOperatorMapping_LongTerm.num_days_ago = yesterday
TestProd_BusMetrics_Fall2025Rating.num_days_ago = yesterday
TestProd_BusOperatorMapping_Fall2025Rating.num_days_ago = yesterday

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
        # TestProd_VehiclePositions_LightRailTerminals_60Day,
        TestProd_TripUpdates_LightRailTerminals_60Day,
        # TestProd_VehiclePositions_HeavyRailTerminals_30Day,
        # TestProd_TripUpdates_HeavyRailTerminals_30Day,
        # TestProd_VehiclePositions_LightRail_7Day,
        # TestDevGreen_VehiclePositions_LightRailTerminals_60Day,
        # TestDevGreen_TripUpdates_LightRailTerminals_60Day,
        # TestDevGreen_VehiclePositions_HeavyRailTerminals_60Day,
        # TestDevGreen_TripUpdates_HeavyRailTerminals_60Day,
        # TestProd_BusOperatorMapping_Recent,
        # TestProd_BusOperatorMapping_LongTerm,
        # TestHyperBus,
        # TestProd_RailMetrics_Subway_LongTerm,
        # TestProd_RailMetrics_CommuterRail_LongTerm,
        # TestHyperBusPerformanceRecent(),
        # TestHyperBusPerformanceAll(),
        # TestProd_BusMetrics_Fall2025Rating,
        # TestProd_BusOperatorMapping_Fall2025Rating,
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
