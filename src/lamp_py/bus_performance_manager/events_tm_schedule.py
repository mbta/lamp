from dataclasses import dataclass
import polars as pl

from lamp_py.runtime_utils.remote_files import (
    tm_geo_node_file,
    tm_route_file,
    tm_trip_file,
    tm_vehicle_file,
    tm_time_point_file,
    tm_pattern_geo_node_xref_file,
)


@dataclass
class DataStats:
    describe: pl.DataFrame
    global_columns: list
    groupby_primary: pl.Expr
    groupby_columns: list
    dataframe_stats_global: pl.DataFrame
    dataframe_stats_group: pl.DataFrame


@dataclass(frozen=True)
class TransitMasterSchedule:
    tm_geo_nodes: pl.DataFrame
    tm_routes: pl.DataFrame
    tm_vehicles: pl.DataFrame
    tm_time_points: pl.DataFrame
    tm_pattern_geo_node_xref: pl.DataFrame
    tm_trip_xref: pl.DataFrame  # derived
    tm_sequences: pl.DataFrame  # derived

    # # eventually just these two
    # tm_schedule: pl.DataFrame
    # tm_schedule_stats: DataStats


def generate_tm_schedule() -> TransitMasterSchedule:
    """

    non service tiemepoints
    vs all timepoints that havent joined
    where avl is bad
    there are places where gtfs is bad at getting stop records
    one examples - silverline tunnel. gtfs rt - picks up on some stops, gtfs-rt is functionally not tracking through SL tunnel. those rows are basically not in lamp
    gtfs rt background
    start schedule - e
    drop any row without stuff at the end
    tm schedule - not for service stops - this will have those

    """
    # the geo node id is the transit master key and the geo node abbr is the
    # gtfs stop id
    tm_geo_nodes = (
        pl.scan_parquet(tm_geo_node_file.s3_uri)
        .select(
            "GEO_NODE_ID",
            "GEO_NODE_ABBR",
        )
        .filter(pl.col("GEO_NODE_ABBR").is_not_null())
        .unique()
    )

    # the route id is the transit master key and the route abbr is the gtfs
    # route id.
    # NOTE: some of these route ids have leading zeros
    tm_routes = (
        pl.scan_parquet(tm_route_file.s3_uri)
        .select(
            "ROUTE_ID",
            "ROUTE_ABBR",
        )
        .filter(pl.col("ROUTE_ABBR").is_not_null())
        .unique()
    )

    # the trip id is the transit master key and the trip serial number is the
    # gtfs trip id.
    tm_trips = (
        pl.scan_parquet(tm_trip_file.s3_uri)
        .select(
            "TRIP_ID",
            "TRIP_SERIAL_NUMBER",
            "Pattern_ID",
        )
        .rename({"Pattern_ID": "PATTERN_ID"})
        .filter(pl.col("TRIP_SERIAL_NUMBER").is_not_null() & pl.col("PATTERN_ID").is_not_null())
        #          "TRIP_SERIAL_NUMBER": "trip_id"})
        .unique()
    )

    # the vehicle id is the transit master key and the property tag is the
    # vehicle label
    tm_vehicles = (
        pl.scan_parquet(tm_vehicle_file.s3_uri)
        .select(
            "VEHICLE_ID",
            "PROPERTY_TAG",
        )
        .filter(pl.col("PROPERTY_TAG").is_not_null())
        .unique()
    )

    tm_time_points = (
        pl.scan_parquet(tm_time_point_file.s3_uri).select(
            "TIME_POINT_ID",
            "TIME_POINT_ABBR",
            "TIME_PT_NAME",
        )
        # .rename({"TIME_POINT_ID": "time_point_id",
        #          "TIME_POINT_ABBR": "time_point_abbr",
        #          "TIME_PT_NAME" : "time_pt_name"})
        # .filter(pl.col("TIME_POINT_ABBR").is_not_null() & pl.col("TIME_PT_NAME").is_not_null())
    )

    tm_pattern_geo_node_xref = (
        pl.scan_parquet(tm_pattern_geo_node_xref_file.s3_uri)
        .select("PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID", "GEO_NODE_ID")
        .filter(
            pl.col("TIME_POINT_ID").is_not_null()
            & pl.col("GEO_NODE_ID").is_not_null()
            & pl.col("PATTERN_ID").is_not_null()
            & pl.col("PATTERN_GEO_NODE_SEQ").is_not_null()
        )
    ).with_columns(
        pl.col(["PATTERN_GEO_NODE_SEQ"]).rank(method="dense").over(["PATTERN_ID"]).alias("timepoint_order"),
    )

    # this is the truth to reference and compare STOP_CROSSING records with to determine timepoint_order
    tm_trip_geo_tp = (
        tm_trips.join(
            tm_pattern_geo_node_xref,
            on="PATTERN_ID",
            how="left",
            coalesce=True,
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID", how="left", coalesce=True)
        .join(tm_time_points, on="TIME_POINT_ID", how="left", coalesce=True)
    )

    # TRIP_ID or [TRIP_SERIAL_NUMBER, PATTERN_ID] uniquely identify a TM "Trip".
    # TRIP_SERIAL_NUMBER is the publicly facing number (and gets aliased to "TRIP_ID" below)
    tm_sequences = tm_trip_geo_tp.group_by(["TRIP_ID"]).agg(
        pl.col("PATTERN_GEO_NODE_SEQ").max().alias("tm_planned_sequence_end"),
        pl.col("PATTERN_GEO_NODE_SEQ").min().alias("tm_planned_sequence_start"),
    )

    # tm_schedule = (
    #     tm_trip_xref
    #     .join(tm_routes,
    #         on="ROUTE_ID",
    #         how="left",
    #         coalesce=True,
    #     )
    #     .join(
    #         tm_vehicles,
    #         on="VEHICLE_ID",
    #         how="left",
    #         coalesce=True,
    #     )
    #     .join(
    #         tm_sequences,
    #         on="TRIP_ID",
    #         how="left",
    #         coalesce=True,
    #     )
    # ).collect()

    return TransitMasterSchedule(
        tm_geo_nodes=tm_geo_nodes,
        # tm_trips=tm_trips,
        tm_routes=tm_routes,
        tm_vehicles=tm_vehicles,
        tm_time_points=tm_time_points,
        tm_pattern_geo_node_xref=tm_pattern_geo_node_xref,
        tm_trip_xref=tm_trip_geo_tp,
        tm_sequences=tm_sequences,
        # tm_schedule=tm_schedule,
    )


#     tm_pattern_geo_node_xref_schema = {
#         "PATTERN_ID": "<class 'int'>",
#         "GEO_NODE_ID": "<class 'int'>",
#         "PATTERN_GEO_NODE_SEQ": "<class 'int'>",
#         "FARE_ZONE_ID": "<class 'int'>",
#         "INTERNAL_ANNOUNCEMENT_ID": "<class 'int'>",
#         "ANNOUNCEMENT_ZONE_ID": "<class 'int'>",
#         "TIME_TABLE_VERSION_ID": "<class 'int'>",
#         "ARRIVAL_ZONE_ID": "<class 'int'>",
#         "DEPARTURE_ZONE_ID": "<class 'int'>",
#         "MDT_ANNUN": "<class 'int'>",
#         "MDT_ARZONE": "<class 'int'>",
#         "MDT_DEZONE": "<class 'int'>",
#         "MDT_ANZONE": "<class 'int'>",
#         "MDT_INCLUDE": "<class 'int'>",
#         "EARLY_ARRIVAL_ALLOWED": "<class 'str'>",
#         "TIME_POINT_ID": "<class 'int'>",
#         "ADHERENCE_POINT": "<class 'bool'>",
#         "REQUIRED_ANNOUNCEMENT": "<class 'int'>",
#         "STOP_NUM": "<class 'int'>",
#     }
#     empty = pl.DataFrame(schema=tm_pattern_geo_node_xref_schema)

#     # /// inputs
#     tm_trip_file = {
#         "TRIP_ID": "<class 'int'>", # plan_trip_id
#         "EXTERNAL_ANNOUNCEMENT_ID": "<class 'int'>",
#         "WORK_PIECE_ID": "<class 'int'>",
#         "TRIP_TYPE_ID": "<class 'int'>",
#         "TIME_TABLE_VERSION_ID": "<class 'int'>",
#         "TRANSIT_DIV_ID": "<class 'int'>",
#         "BLOCK_ID": "<class 'int'>",
#         "TRIP_SERIAL_NUMBER": "<class 'int'>",
#         "TRIP_SEQUENCE": "<class 'int'>",
#         "REMARK": "<class 'str'>",
#         "BLOCK_TRIP_SEQ": "<class 'int'>",
#         "Pattern_ID": "<class 'int'>",
#         "TRIP_START_NODE_ID": "<class 'int'>",
#         "TRIP_END_TIME": "<class 'int'>",
#         "TRIP_END_NODE_ID": "<class 'int'>",
#         "SOURCE_TRIP_ID": "<class 'int'>",
#         "EXC_COMBO_ID": "<class 'int'>",
#     }
#     tm_geo_nodes_schema = {
#         "GEO_NODE_ID": "<class 'int'>",
#         "DEPARTURE_ZONE_ID": "<class 'int'>",
#         "ARRIVAL_ZONE_ID": "<class 'int'>",
#         "INTERNAL_ANNOUNCEMENT_ID": "<class 'int'>",
#         "GEO_NODE_ABBR": "<class 'str'>",
#         "GEO_NODE_NAME": "<class 'str'>",
#         "TRANSIT_DIV_ID": "<class 'str'>",
#         "LATITUDE": "<class 'int'>",
#         "LONGITUDE": "<class 'int'>",
#         "ALTITUDE": "<class 'str'>",
#         "DETOUR_IND": "<class 'str'>",
#         "DETOUR_LATITUDE": "<class 'int'>",
#         "DETOUR_LONGITIUDE": "<class 'int'>",
#         "REMARK": "<class 'str'>",
#         "DIFFERENTIAL_VALIDITY": "<class 'int'>",
#         "NUM_OF_SATELLITES": "<class 'int'>",
#         "ACTIVATION_DATE": "<class 'str'>",
#         "DEACTIVATION_DATE": "<class 'str'>",
#         "MAP_LATITUDE": "<class 'int'>",
#         "MAP_LONGITUDE": "<class 'int'>",
#         "ANNOUNCEMENT_ZONE_ID": "<class 'int'>",
#         "NUM_SURVEY_POINTS": "<class 'int'>",
#         "USE_SURVEY": "<class 'bool'>",
#         "MDT_LATITUDE": "<class 'int'>",
#         "MDT_LONGITUDE": "<class 'int'>",
#         "LOCK_MAP": "<class 'bool'>",
#         "GEO_NODE_PUBLIC_NAME": "<class 'str'>",
#         "SOURCE_STOP_ID": "<class 'int'>",
#         "PUBLIC_STOP_NUMBER": "<class 'str'>",
#         "BAY_INFO": "<class 'str'>",
#     }

#     tm_trip_xref_schema = {
#         "TRIP_ID": "<class 'int'>",
#         "TRIP_SERIAL_NUMBER": "<class 'int'>",
#         "PATTERN_ID": "<class 'int'>",
#         "PATTERN_GEO_NODE_SEQ": "<class 'int'>",
#         "TIME_POINT_ID": "<class 'int'>",
#         "GEO_NODE_ID": "<class 'int'>",
#         "timepoint_order": "<class 'int'>",
#         "GEO_NODE_ABBR": "<class 'str'>",
#         "TIME_POINT_ABBR": "<class 'str'>",
#         "TIME_PT_NAME": "<class 'str'>",
#     }

#     tm_pattern_geo_node_xref = {
#         "PATTERN_ID": "<class 'int'>",
#         "GEO_NODE_ID": "<class 'int'>",
#         "PATTERN_GEO_NODE_SEQ": "<class 'int'>",
#         "FARE_ZONE_ID": "<class 'int'>",
#         "INTERNAL_ANNOUNCEMENT_ID": "<class 'int'>",
#         "ANNOUNCEMENT_ZONE_ID": "<class 'int'>",
#         "TIME_TABLE_VERSION_ID": "<class 'int'>",
#         "ARRIVAL_ZONE_ID": "<class 'int'>",
#         "DEPARTURE_ZONE_ID": "<class 'int'>",
#         "MDT_ANNUN": "<class 'int'>",
#         "MDT_ARZONE": "<class 'int'>",
#         "MDT_DEZONE": "<class 'int'>",
#         "MDT_ANZONE": "<class 'int'>",
#         "MDT_INCLUDE": "<class 'int'>",
#         "EARLY_ARRIVAL_ALLOWED": "<class 'str'>",
#         "TIME_POINT_ID": "<class 'int'>",
#         "ADHERENCE_POINT": "<class 'bool'>",
#         "REQUIRED_ANNOUNCEMENT": "<class 'int'>",
#         "STOP_NUM": "<class 'int'>",
#     }

#     routes = {
#         "ROUTE_ID": "<class 'int'>",
#         "ROUTE_GROUP_ID": "<class 'int'>",
#         "ROUTE_ABBR": "<class 'str'>",
#         "ROUTE_NAME": "<class 'str'>",
#         "TRANSIT_DIV_ID": "<class 'int'>",
#         "TIME_TABLE_VERSION_ID": "<class 'int'>",
#         "SOURCE_LINE_ID": "<class 'int'>",
#         "MASTER_ROUTE_ID": "<class 'int'>",
#     }
#     tm_sequences = {
#         "TRIP_ID": "<class 'int'>",
#         "tm_planned_sequence_end": "<class 'int'>", #TRIP_START_NODE_ID
#         "tm_planned_sequence_start": "<class 'int'>", #TRIP_END_NODE_ID
#     }

#     tm_timepoint = {
#         "TIME_POINT_ID": "<class 'int'>",
#         "TIME_POINT_ABBR": "<class 'str'>",
#         "TIME_PT_NAME": "<class 'str'>",
#         "SOURCE_NODE_ID": "<class 'int'>",
#     }

#     tm_vehicles = {
#         "VEHICLE_ID": "<class 'int'>",
#         "FLEET_ID": "<class 'int'>",
#         "TRANSIT_DIV_ID": "<class 'int'>",
#         "PROPERTY_TAG": "<class 'str'>",
#         "MFG_MODEL_SERIES_ID": "<class 'int'>",
#         "VEH_SERVICE_STATUS_ID": "<class 'int'>",
#         "VEHICLE_TYPE_ID": "<class 'int'>",
#         "VEHICLE_BASE_ID": "<class 'int'>",
#         "RNET_ADDRESS": "<class 'int'>",
#         "VIN": "<class 'str'>",
#         "LICENSE_PLATE": "<class 'str'>",
#         "MODEL_YEAR": "<class 'int'>",
#         "ODOMETER_MILEAGE": "<class 'int'>",
#         "ODOMTR_UPDATE_TIMESTAMP": "<class 'datetime.datetime'>",
#         "ODOMTR_UPDATE_SOURCE_IND": "<class 'str'>",
#         "GENERAL_INFORMATION": "<class 'str'>",
#         "LOAD_ID": "<class 'int'>",
#         "RADIO_LID": "<class 'int'>",
#         "ENGINE_CONTROLLER_ID": "<class 'int'>",
#         "DEFAULT_VOICE_CHANNEL": "<class 'int'>",
#         "USE_DHCP": "<class 'bool'>",
#         "IP_OCT_1": "<class 'int'>",
#         "IP_OCT_2": "<class 'int'>",
#         "IP_OCT_3": "<class 'int'>",
#         "IP_OCT_4": "<class 'int'>",
#         "EEPROM_TEMPLATE_ID": "<class 'int'>",
#         "DECOMMISSION": "<class 'bool'>",
#         "VEHICLE_TYPE_DESCRIPTION_ID": "<class 'str'>",
#         "APC_TYPE_ID": "<class 'int'>",
#         "RADIO_CONFIGURATION_ID": "<class 'int'>",
#         "VOICE_CAPABILITY": "<class 'int'>",
#         "IVLU_FILE_PACKAGING": "<class 'int'>",
#         "IVLU_WAKEUP_MESSAGE": "<class 'int'>",
#         "APC_ENABLED": "<class 'bool'>",
#         "COLLECT_APC_DATA": "<class 'bool'>",
#         "TSP_DEVICE_ID": "<class 'str'>",
#         "RADIO_DEVICE_ID": "<class 'str'>",
#     }

# # get all the TRIP_ID that are empty - empty being if all the XYZ for a given TRIP id are empty,


# OG

# .join(
#     tm_scheduled.tm_routes,
#     on="ROUTE_ID",
#     how="left",
#     coalesce=True,
# )
# .join(
#     tm_scheduled.tm_vehicles,
#     on="VEHICLE_ID",
#     how="left",
#     coalesce=True,
# )
# .join(
#     tm_scheduled.tm_sequences,
#     on="TRIP_ID",
#     how="left",
#     coalesce=True,
# )
# .join(
#     tm_scheduled.tm_trip_xref,
#     on=["TRIP_ID", "TIME_POINT_ID", "GEO_NODE_ID", "PATTERN_GEO_NODE_SEQ"],
#     how="left",
#     coalesce=True,
# )
