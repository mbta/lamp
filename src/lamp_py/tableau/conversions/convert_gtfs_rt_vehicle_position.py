import polars as pl
import pyarrow

gtfs_rt_vehicle_positions_processed_schema = pyarrow.schema(
    [
        ("id", pyarrow.large_string()),
        ("vehicle.trip.trip_id", pyarrow.large_string()),
        ("vehicle.trip.route_id", pyarrow.large_string()),
        ("vehicle.trip.direction_id", pyarrow.uint8()),
        ("vehicle.trip.start_time", pyarrow.large_string()),
        ("vehicle.trip.start_date", pyarrow.large_string()),
        ("vehicle.trip.schedule_relationship", pyarrow.large_string()),
        ("vehicle.trip.route_pattern_id", pyarrow.large_string()),
        ("vehicle.trip.tm_trip_id", pyarrow.large_string()),
        ("vehicle.trip.overload_id", pyarrow.int64()),
        ("vehicle.trip.overload_offset", pyarrow.int64()),
        ("vehicle.trip.revenue", pyarrow.bool_()),
        ("vehicle.trip.last_trip", pyarrow.bool_()),
        ("vehicle.vehicle.id", pyarrow.large_string()),
        ("vehicle.vehicle.label", pyarrow.large_string()),
        ("vehicle.vehicle.license_plate", pyarrow.large_string()),
        # vehicle.vehicle.consist: list<element: struct<label: string>>
        #   child 0, element: struct<label: string>
        #       child 0, label: string
        ("vehicle.vehicle.assignment_status", pyarrow.large_string()),
        ("vehicle.position.bearing", pyarrow.uint16()),
        ("vehicle.position.latitude", pyarrow.float64()),
        ("vehicle.position.longitude", pyarrow.float64()),
        ("vehicle.position.speed", pyarrow.float64()),
        ("vehicle.position.odometer", pyarrow.float64()),
        ("vehicle.current_stop_sequence", pyarrow.uint32()),
        ("vehicle.stop_id", pyarrow.large_string()),
        ("vehicle.current_status", pyarrow.large_string()),
        ("vehicle.timestamp", pyarrow.timestamp("ms")),
        ("vehicle.congestion_level", pyarrow.large_string()),
        ("vehicle.occupancy_status", pyarrow.large_string()),
        ("vehicle.occupancy_percentage", pyarrow.uint32()),
        # vehicle.multi_carriage_details: list<element: struct<id: string, label: string, occupancy_status: string, occupancy_percentage: int32, carriage_sequence: uint32>>
        #   child 0, element: struct<id: string, label: string, occupancy_status: string, occupancy_percentage: int32, carriage_sequence: uint32>
        #       child 0, id: string
        #       child 1, label: string
        #       child 2, occupancy_status: string
        #       child 3, occupancy_percentage: int32
        #       child 4, carriage_sequence: uint32
        ("feed_timestamp", pyarrow.timestamp("ms")),
    ]
)


def apply_gtfs_rt_vehicle_positions_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    polars_df = apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df)
    polars_df = apply_gtfs_vehicle_positions_lrtp(polars_df)

    return polars_df


def apply_gtfs_vehicle_positions_lrtp(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply lrtp filters conversions to lamp data before outputting for tableau consumption
    """
    terminal_stop_ids = list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))

    #    pylint: disable=singleton-comparison
    polars_df = polars_df.filter(
        ~pl.col("vehicle.timestamp").is_null()
        & pl.col("vehicle.stop_id").is_in(terminal_stop_ids)
        & pl.col("vehicle.trip.revenue")
        == True
    )
    return polars_df


def apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    polars_df = polars_df.with_columns(
        pl.from_epoch(pl.col("vehicle.timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
    )
    return polars_df


def schema() -> pyarrow.schema:
    """
    Function returns schema of the processed gtfs-rt vehicle positions
    """
    return gtfs_rt_vehicle_positions_processed_schema
