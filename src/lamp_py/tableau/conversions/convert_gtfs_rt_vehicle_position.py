import polars as pl
import dataframely as dy

from lamp_py.utils.filter_bank import FilterBankRtVehiclePositions
from lamp_py.runtime_utils.process_logger import ProcessLogger


class VehiclePositions(dy.Schema):
    "All fields from VehiclePositions message."
    id = dy.String(nullable=True)
    trip_id = dy.String(nullable=True, alias="vehicle.trip.trip_id")
    route_id = dy.String(nullable=True, alias="vehicle.trip.route_id")
    direction_id = dy.UInt8(nullable=True, alias="vehicle.trip.direction_id")
    start_time = dy.String(nullable=True, alias="vehicle.trip.start_time")
    start_date = dy.String(nullable=True, alias="vehicle.trip.start_date")
    revenue = dy.Bool(nullable=True, alias="vehicle.trip.revenue")
    vehicle_id = dy.String(nullable=True, alias="vehicle.vehicle.id")
    vehicle_label = dy.String(nullable=True, alias="vehicle.vehicle.label")
    timestamp = dy.Datetime(
        nullable=True,
        alias="vehicle.timestamp",
        check=lambda x: x.is_not_null(),  # setting field nullability directly prevents writing with pyarrow; remove explicit pyarrow schema validation once all datasets use dataframely validation
    )
    feed_timestamp = dy.Datetime(nullable=True)
    stop_id = dy.String(nullable=True, alias="vehicle.stop_id")
    route_pattern_id = dy.String(nullable=True, alias="vehicle.trip.route_pattern_id")
    tm_trip_id = dy.String(nullable=True, alias="vehicle.trip.tm_trip_id")
    overload_id = dy.Int64(nullable=True, alias="vehicle.trip.overload_id")
    overload_offset = dy.Int64(nullable=True, alias="vehicle.trip.overload_offset")
    last_trip = dy.Bool(nullable=True, alias="vehicle.trip.last_trip")
    schedule_relationship = dy.String(nullable=True, alias="vehicle.trip.schedule_relationship")
    license_plate = dy.String(nullable=True, alias="vehicle.vehicle.license_plate")
    assignment_status = dy.String(nullable=True, alias="vehicle.vehicle.assignment_status")
    bearing = dy.UInt16(nullable=True, alias="vehicle.position.bearing")
    latitude = dy.Float64(nullable=True, alias="vehicle.position.latitude")
    longitude = dy.Float64(nullable=True, alias="vehicle.position.longitude")
    speed = dy.Float64(nullable=True, alias="vehicle.position.speed")
    odometer = dy.Float64(nullable=True, alias="vehicle.position.odometer")
    current_stop_sequence = dy.UInt32(nullable=True, alias="vehicle.current_stop_sequence")
    congestion_level = dy.String(nullable=True, alias="vehicle.congestion_level")
    occupancy_status = dy.String(nullable=True, alias="vehicle.occupancy_status")
    occupancy_percentage = dy.UInt32(nullable=True, alias="vehicle.occupancy_percentage")
    current_status = dy.String(nullable=True, alias="vehicle.current_status")


class LightRailTerminalVehiclePositions(dy.Schema):
    "Analytical VehiclePositions dataset for light rail terminal predictions."
    id = VehiclePositions.id
    trip_id = VehiclePositions.trip_id
    route_id = VehiclePositions.route_id
    direction_id = VehiclePositions.direction_id
    start_time = VehiclePositions.start_time
    start_date = VehiclePositions.start_date
    revenue = VehiclePositions.revenue
    vehicle_id = VehiclePositions.vehicle_id
    vehicle_label = VehiclePositions.vehicle_label
    timestamp = VehiclePositions.timestamp
    feed_timestamp = VehiclePositions.feed_timestamp
    stop_id = VehiclePositions.stop_id
    stop_id = dy.String(
        nullable=True,
        alias="vehicle.stop_id",
        check=lambda x: x.is_in(FilterBankRtVehiclePositions.ParquetFilter.light_rail_terminal_stop_list),
    )
    trip_id = dy.String(nullable=True, alias="vehicle.trip.trip_id", check=lambda x: x.is_not_null())
    revenue = dy.Bool(nullable=True, alias="vehicle.trip.revenue", check=lambda x: x)
    feed_timestamp = dy.Datetime(nullable=True, check=lambda x: x.is_not_null())


class HeavyRailTerminalVehiclePositions(VehiclePositions):
    "Analytical dataset for heavy rail and light rail midpoint dashboards."
    stop_id = dy.String(
        nullable=True,
        alias="vehicle.stop_id",
        check=lambda x: x.is_in(FilterBankRtVehiclePositions.ParquetFilter.heavy_rail_terminal_stop_list),
    )


def lrtp(polars_df: pl.DataFrame) -> dy.DataFrame[LightRailTerminalVehiclePositions]:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    process_logger = ProcessLogger("lrtp")
    process_logger.log_start()

    def lrtp_restrict_vp_to_only_terminal_stop_ids(polars_df: pl.DataFrame) -> pl.DataFrame:
        """
        Function to apply lrtp filters conversions to lamp data before outputting for tableau consumption
        """

        #    pylint: disable=singleton-comparison
        polars_df = polars_df.filter(
            pl.col("vehicle.timestamp").is_not_null(),
            pl.col("vehicle.stop_id").is_in(FilterBankRtVehiclePositions.ParquetFilter.light_rail_terminal_stop_list),
            pl.col("vehicle.trip.revenue"),
            pl.col("vehicle.trip.trip_id").is_not_null(),
            pl.col("feed_timestamp").is_not_null(),
        )
        return polars_df

    polars_df = lrtp_restrict_vp_to_only_terminal_stop_ids(polars_df)
    polars_df = apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df)
    valid = LightRailTerminalVehiclePositions.validate(polars_df)

    process_logger.log_start()

    return valid


def heavyrail(polars_df: pl.DataFrame) -> dy.DataFrame[HeavyRailTerminalVehiclePositions]:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    process_logger = ProcessLogger("heavyrail")
    process_logger.log_start()

    polars_df = apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df)
    polars_df = apply_gtfs_vehicle_positions_heavy(polars_df)

    valid = HeavyRailTerminalVehiclePositions.validate(polars_df)

    process_logger.log_complete()

    return valid


def apply_gtfs_vehicle_positions_heavy(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply lrtp filters conversions to lamp data before outputting for tableau consumption
    """

    #    pylint: disable=singleton-comparison
    polars_df = polars_df.filter(
        ~pl.col("vehicle.timestamp").is_null()
        & pl.col("vehicle.stop_id").is_in(FilterBankRtVehiclePositions.ParquetFilter.heavy_rail_terminal_stop_list)
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
