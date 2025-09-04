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
        ("vehicle.timestamp", pyarrow.timestamp("us")),
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
        ("feed_timestamp", pyarrow.timestamp("us")),
    ]
)


def lrtp(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """

    def lrtp_restrict_vp_to_only_terminal_stop_ids(polars_df: pl.DataFrame) -> pl.DataFrame:
        """
        Function to apply lrtp filters conversions to lamp data before outputting for tableau consumption
        """
        stop_ids = list(map(str, [70110, 70162, 70236, 70274, 70502, 70510]))

        #    pylint: disable=singleton-comparison
        polars_df = polars_df.filter(~pl.col("vehicle.timestamp").is_null() & pl.col("vehicle.stop_id").is_in(stop_ids))
        return polars_df

    def temporary_lrtp_assign_new_trip_ids(polars_df: pl.DataFrame, threshold_sec: int = 60 * 15) -> pl.DataFrame:
        """
        Function to apply temporary trip ids to trips that have the same trip_id assigned for improbable stop durations at a single station

        THIS IS NOT PERMANENT - THIS SHOULD BE REMOVED WHEN THE TRIP_IDS ARE PROPERLY POPULATED UPSTREAM

        """
        polars_df = (
            polars_df.sort("feed_timestamp")
            .with_columns(
                pl.col("feed_timestamp").diff().alias("mdiff").over("vehicle.trip.trip_id", "vehicle.trip.start_date")
            )
            .with_columns(
                pl.when(pl.col("mdiff") < threshold_sec)
                .then(0)
                .otherwise(1)
                .fill_null(0)
                .cum_sum()
                .cast(pl.String)
                .alias("new_id")
                .over("vehicle.trip.trip_id", "vehicle.trip.start_date")
            )
            .with_columns(
                pl.when(pl.col("new_id").ne("1"))
                .then(
                    pl.concat_str(
                        [pl.col("vehicle.trip.trip_id"), pl.lit("_LAMP"), pl.col("new_id")], ignore_nulls=True
                    )
                )
                .alias("vehicle.trip.trip_id1")
            )
            .with_columns(pl.coalesce("vehicle.trip.trip_id1", "vehicle.trip.trip_id").alias("vehicle.trip.trip_id"))
            .drop("vehicle.trip.trip_id1")
        )
        return polars_df

    polars_df = lrtp_restrict_vp_to_only_terminal_stop_ids(polars_df)
    # after we have filtered to only terminal stop_ids, then check that the trip_id vs timestamps make sense, and
    # assign new trip IDs if it doesn't
    polars_df = temporary_lrtp_assign_new_trip_ids(polars_df)
    polars_df = apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df)

    return polars_df


def heavyrail(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    polars_df = apply_gtfs_rt_vehicle_positions_timezone_conversions(polars_df)
    polars_df = apply_gtfs_vehicle_positions_heavy(polars_df)

    return polars_df


def apply_gtfs_vehicle_positions_heavy(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply lrtp filters conversions to lamp data before outputting for tableau consumption
    """
    stop_ids = list(map(str, [70003, 70034, 70040, 70057, 70063, 70092, 70104]))

    #    pylint: disable=singleton-comparison
    polars_df = polars_df.filter(~pl.col("vehicle.timestamp").is_null() & pl.col("vehicle.stop_id").is_in(stop_ids))
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
