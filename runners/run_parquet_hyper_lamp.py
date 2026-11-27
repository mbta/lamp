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
    PROD_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY,
    PROD_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY,
    PROD_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY,
    PROD_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_30_DAY,
    PROD_VEHICLE_POSITIONS_LIGHT_RAIL_7_DAY,
    DEV_GREEN_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY,
    DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY,
    DEV_GREEN_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY,
    DEV_GREEN_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_60_DAY,
    PROD_BUS_OPERATOR_MAPPING_RECENT,
    PROD_BUS_OPERATOR_MAPPING_LONG_TERM,
    PROD_BUS_METRICS_FALL2025_RATING,
    PROD_BUS_OPERATOR_MAPPING_FALL2025_RATING,
    PROD_RAIL_METRICS_SUBWAY_LONG_TERM,
    PROD_RAIL_METRICS_COMMUTER_RAIL_LONG_TERM,
)

TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY = PROD_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY
TEST_PROD_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY = PROD_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY
TEST_PROD_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY = PROD_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY
TEST_PROD_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_30_DAY = PROD_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_30_DAY
TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_7_DAY = PROD_VEHICLE_POSITIONS_LIGHT_RAIL_7_DAY
TEST_DEV_GREEN_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY = DEV_GREEN_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY

TEST_DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY = DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY
TEST_DEV_GREEN_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY = DEV_GREEN_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY
TEST_DEV_GREEN_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_60_DAY = DEV_GREEN_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_60_DAY
TEST_PROD_BUS_OPERATOR_MAPPING_RECENT = PROD_BUS_OPERATOR_MAPPING_RECENT
TEST_PROD_BUS_OPERATOR_MAPPING_LONG_TERM = PROD_BUS_OPERATOR_MAPPING_LONG_TERM
TEST_PROD_BUS_METRICS_FALL2025_RATING = PROD_BUS_METRICS_FALL2025_RATING
TEST_PROD_BUS_OPERATOR_MAPPING_FALL2025_RATING = PROD_BUS_OPERATOR_MAPPING_FALL2025_RATING
TEST_PROD_RAIL_METRICS_SUBWAY_LONG_TERM = PROD_RAIL_METRICS_SUBWAY_LONG_TERM
TEST_PROD_RAIL_METRICS_COMMUTER_RAIL_LONG_TERM = PROD_RAIL_METRICS_COMMUTER_RAIL_LONG_TERM
TestHyperBusPerformanceRecent = HyperBusPerformanceRecent
TestHyperBusPerformanceAll = HyperBusPerformanceAll

yesterday = 1
TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_PROD_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_PROD_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_PROD_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_30_DAY.num_days_ago = yesterday
TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_7_DAY.num_days_ago = yesterday
TEST_DEV_GREEN_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_DEV_GREEN_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_DEV_GREEN_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_60_DAY.num_days_ago = yesterday
TEST_PROD_BUS_OPERATOR_MAPPING_RECENT.num_days_ago = yesterday
TEST_PROD_BUS_OPERATOR_MAPPING_LONG_TERM.num_days_ago = yesterday
TEST_PROD_BUS_METRICS_FALL2025_RATING.num_days_ago = yesterday
TEST_PROD_BUS_OPERATOR_MAPPING_FALL2025_RATING.num_days_ago = yesterday

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
        # DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY
        # TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY,
        # TEST_PROD_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY,
        # TEST_PROD_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY,
        # TEST_PROD_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_30_DAY,
        TEST_PROD_VEHICLE_POSITIONS_LIGHT_RAIL_7_DAY,
        # TEST_DEV_GREEN_VEHICLE_POSITIONS_LIGHT_RAIL_TERMINALS_60_DAY,
        # TEST_DEV_GREEN_TRIP_UPDATES_LIGHT_RAIL_TERMINALS_60_DAY,
        # TEST_DEV_GREEN_VEHICLE_POSITIONS_HEAVY_RAIL_TERMINALS_60_DAY,
        # TEST_DEV_GREEN_TRIP_UPDATES_HEAVY_RAIL_TERMINALS_60_DAY,
        # TEST_PROD_BUS_OPERATOR_MAPPING_RECENT,
        # TEST_PROD_BUS_OPERATOR_MAPPING_LONG_TERM,
        # TestHyperBus,
        # TEST_PROD_RAIL_METRICS_SUBWAY_LONG_TERM,
        # TEST_PROD_RAIL_METRICS_COMMUTER_RAIL_LONG_TERM,
        # TestHyperBusPerformanceRecent(),
        # TestHyperBusPerformanceAll(),
        # TEST_PROD_BUS_METRICS_FALL2025_RATING,
        # TEST_PROD_BUS_OPERATOR_MAPPING_FALL2025_RATING,
    ]

    local_parquet = False
    run_pq_remote = False
    local_hyper = False
    run_hyper_remote = True

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
