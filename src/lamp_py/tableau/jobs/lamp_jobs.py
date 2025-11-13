import os
from lamp_py.bus_performance_manager.events_joined import TMDailyWorkPiece
from lamp_py.tableau.conversions import (
    convert_gtfs_rt_trip_updates,
    convert_gtfs_rt_vehicle_position,
)

from lamp_py.runtime_utils.remote_files import (
    springboard_rt_vehicle_positions,
    springboard_devgreen_rt_vehicle_positions,
    springboard_rt_trip_updates,  # main feed, all lines, unique records
    springboard_devgreen_lrtp_trip_updates,  # dev green feed, green line only, all records
    springboard_lrtp_trip_updates,  # main feed, green line only, all records
    bus_operator_mapping,
    tableau_rt_vehicle_positions_lightrail_60_day,
    tableau_rt_trip_updates_lightrail_60_day,
    tableau_rt_vehicle_positions_heavyrail_30_day,
    tableau_rt_trip_updates_heavyrail_30_day,
    tableau_devgreen_rt_vehicle_positions_lightrail_60_day,
    tableau_devgreen_rt_trip_updates_lightrail_60_day,
    tableau_rt_vehicle_positions_all_light_rail_7_day,
    tableau_bus_operator_mapping_recent,
    tableau_bus_operator_mapping_all,
    tableau_rail_commuter,
    tableau_rail_subway,
)

from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.rt_rail import HyperRtRail
from lamp_py.utils.filter_bank import FilterBankRtTripUpdates, FilterBankRtVehiclePositions

GTFS_RT_TABLEAU_PROJECT = "GTFS-RT"
LAMP_API_PROJECT = "LAMP API"
# LAMP_API = "LAMP API"
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
    remote_input_location=springboard_lrtp_trip_updates,
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

HyperGtfsRtVehiclePositionsAllLightRail = FilteredHyperJob(
    remote_input_location=springboard_rt_vehicle_positions,
    remote_output_location=tableau_rt_vehicle_positions_all_light_rail_7_day,
    rollup_num_days=7,
    processed_schema=convert_gtfs_rt_vehicle_position.VehiclePositions.pyarrow_schema(),
    dataframe_filter=convert_gtfs_rt_vehicle_position.apply_gtfs_rt_vehicle_positions_timezone_conversions,
    parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
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
    remote_input_location=springboard_devgreen_lrtp_trip_updates,
    remote_output_location=tableau_devgreen_rt_trip_updates_lightrail_60_day,
    rollup_num_days=60,
    processed_schema=convert_gtfs_rt_trip_updates.LightRailTerminalTripUpdates.pyarrow_schema(),
    dataframe_filter=convert_gtfs_rt_trip_updates.lrtp_devgreen,
    parquet_filter=FilterBankRtTripUpdates.ParquetFilter.light_rail,
    tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
)

HyperBusOperatorMappingRecent = FilteredHyperJob(
    remote_input_location=bus_operator_mapping,
    remote_output_location=tableau_bus_operator_mapping_recent,
    rollup_num_days=7,
    processed_schema=TMDailyWorkPiece.pyarrow_schema(),
    dataframe_filter=None,
    parquet_filter=None,
    tableau_project_name=LAMP_API_PROJECT,
    partition_template="",
)

HyperBusOperatorMappingAll = FilteredHyperJob(
    remote_input_location=bus_operator_mapping,
    remote_output_location=tableau_bus_operator_mapping_all,
    rollup_num_days=60,
    processed_schema=TMDailyWorkPiece.pyarrow_schema(),
    dataframe_filter=None,
    parquet_filter=None,
    tableau_project_name=LAMP_API_PROJECT,
    partition_template="",
)

HyperRtRailSubway = (
    HyperRtRail(
        route_type_operator="<",
        route_type_operand="2",  # enum 1 = local rail
        hyper_file_name=os.path.join(tableau_rail_subway.s3_uri, "LAMP_RAIL_RT_fields.hyper"),
        remote_parquet_path=os.path.join(tableau_rail_subway.s3_uri, "LAMP_RAIL_RT_fields.parquet"),
        lamp_version="1.2.2",
    ),
)  # light rail and heavy rail

HyperRtRailCommuter = (
    HyperRtRail(
        route_type_operator="=",
        route_type_operand="2",  # enum 2 = commuter rail
        hyper_file_name=os.path.join(tableau_rail_commuter.s3_uri, "LAMP_COMMUTER_RAIL_RT_fields.hyper"),
        remote_parquet_path=os.path.join(tableau_rail_commuter.s3_uri, "LAMP_COMMUTER_RAIL_RT_fields.parquet"),
        lamp_version="1.0.0",
    ),
)  # commuter rail
