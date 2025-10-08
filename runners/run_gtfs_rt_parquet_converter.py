import os
from typing import List

from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates, convert_gtfs_rt_vehicle_position
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.pipeline import (
    GTFS_RT_TABLEAU_PROJECT,
    HyperGtfsRtVehiclePositions,
    HyperGtfsRtTripUpdates,
    HyperDevGreenGtfsRtVehiclePositions,
    HyperDevGreenGtfsRtTripUpdates,
    HyperGtfsRtTripUpdatesHeavyRail,
    HyperGtfsRtVehiclePositionsHeavyRail,
)

from lamp_py.runtime_utils.remote_files import (
    LAMP,
    S3_SPRINGBOARD,
    S3Location,
    springboard_rt_vehicle_positions,
    springboard_devgreen_rt_vehicle_positions,
    springboard_devgreen_rt_trip_updates,
    springboard_rt_trip_updates,
    tableau_rt_vehicle_positions_lightrail_60_day,
    tableau_rt_trip_updates_lightrail_60_day,
    tableau_rt_vehicle_positions_heavyrail_30_day,
    tableau_rt_trip_updates_heavyrail_30_day,
    tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    tableau_devgreen_rt_trip_updates_lightrail_60_day,
    tableau_rt_vehicle_positions_all_light_rail_7_day,
)

from lamp_py.utils.filter_bank import FilterBankRtTripUpdates, FilterBankRtVehiclePositions


# don't run this in pytest - environment variables in pyproject.toml point to local SPRINGBOARD/ARCHIVE
# need the .env values to run
def start_devgreen_gtfs_rt_parquet_updates_local() -> None:
    """Run all gtfs_rt Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [HyperGtfsRtTripUpdates]

    for job in parquet_update_jobs:
        job.run_parquet(None)


def start_devgreen_gtfs_rt_parquet_updates_local_hyper() -> None:
    """Run all gtfs_rt Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [HyperDevGreenGtfsRtVehiclePositions, HyperDevGreenGtfsRtTripUpdates]

    for job in parquet_update_jobs:
        job.run_parquet(None)
        outs = job.create_local_hyper()
        print(outs)


def start_gtfs_rt_parquet_updates_local() -> None:
    """Run all gtfs_rt Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [HyperGtfsRtVehiclePositions, HyperGtfsRtTripUpdates]

    for job in parquet_update_jobs:
        # breakpoint()
        job.run_parquet(None)
        # outs = job.create_local_hyper()
        # print(outs)


if __name__ == "__main__":
    # start_gtfs_rt_parquet_updates_local()
    # start_devgreen_gtfs_rt_parquet_updates_local()
    # start_devgreen_gtfs_rt_parquet_updates_local_hyper()

    springboard_rt_vehicle_positions.bucket = "mbta-ctd-dataplatform-staging-springboard"
    springboard_devgreen_rt_vehicle_positions.bucket = "mbta-ctd-dataplatform-staging-springboard"
    springboard_rt_trip_updates.bucket = "mbta-ctd-dataplatform-staging-springboard"
    springboard_devgreen_rt_trip_updates.bucket = "mbta-ctd-dataplatform-staging-springboard"
    # rollup_days = 5
    HyperGtfsRtVehiclePositions = FilteredHyperJob(
        remote_input_location=springboard_rt_vehicle_positions,
        remote_output_location=tableau_rt_vehicle_positions_lightrail_60_day,
        rollup_num_days=60,
        processed_schema=convert_gtfs_rt_vehicle_position.LightRailTerminalVehiclePositions.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperGtfsRtTripUpdates = FilteredHyperJob(
        remote_input_location=springboard_rt_trip_updates,
        remote_output_location=tableau_rt_trip_updates_lightrail_60_day,
        rollup_num_days=60,
        processed_schema=convert_gtfs_rt_trip_updates.LightRailTerminalTripUpdates.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_prod,
        parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperGtfsRtVehiclePositionsHeavyRail = FilteredHyperJob(
        remote_input_location=springboard_rt_vehicle_positions,
        remote_output_location=tableau_rt_vehicle_positions_heavyrail_30_day,
        rollup_num_days=30,
        processed_schema=convert_gtfs_rt_vehicle_position.HeavyRailTerminalVehiclePositions.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.heavyrail,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.heavy_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperGtfsRtTripUpdatesHeavyRail = FilteredHyperJob(
        remote_input_location=springboard_rt_trip_updates,
        remote_output_location=tableau_rt_trip_updates_heavyrail_30_day,
        rollup_num_days=30,
        processed_schema=convert_gtfs_rt_trip_updates.HeavyRailTerminalTripUpdates.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_trip_updates.heavyrail,
        parquet_filter=FilterBankRtTripUpdates.ParquetFilter.heavy_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperDevGreenGtfsRtVehiclePositions = FilteredHyperJob(
        remote_input_location=springboard_devgreen_rt_vehicle_positions,
        remote_output_location=tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
        rollup_num_days=60,
        processed_schema=convert_gtfs_rt_vehicle_position.LightRailTerminalVehiclePositions.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperDevGreenGtfsRtTripUpdates = FilteredHyperJob(
        remote_input_location=springboard_devgreen_rt_trip_updates,
        remote_output_location=tableau_devgreen_rt_trip_updates_lightrail_60_day,
        rollup_num_days=60,
        processed_schema=convert_gtfs_rt_trip_updates.LightRailTerminalTripUpdates.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_devgreen,
        parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    HyperGtfsRtVehiclePositionsAllLightRail = FilteredHyperJob(
        remote_input_location=springboard_rt_vehicle_positions,
        remote_output_location=tableau_rt_vehicle_positions_all_light_rail_7_day,
        rollup_num_days=7,
        processed_schema=convert_gtfs_rt_vehicle_position.LightRailTerminalVehiclePositions.pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.apply_gtfs_rt_vehicle_positions_timezone_conversions,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    parquet_update_jobs: List[HyperJob] = [
        HyperGtfsRtVehiclePositions,
        HyperGtfsRtTripUpdates,
        HyperDevGreenGtfsRtVehiclePositions,
        HyperDevGreenGtfsRtTripUpdates,
        HyperGtfsRtVehiclePositionsHeavyRail,
        HyperGtfsRtTripUpdatesHeavyRail,
        HyperGtfsRtVehiclePositionsAllLightRail,
    ]

    for job in parquet_update_jobs:
        job.run_parquet(None)
        breakpoint()
