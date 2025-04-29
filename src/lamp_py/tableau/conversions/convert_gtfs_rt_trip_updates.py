import pyarrow
import polars as pl

gtfs_rt_trip_updates_processed_schema = pyarrow.schema(
    [
        ("id", pyarrow.large_string()),
        ("trip_update.trip.trip_id", pyarrow.large_string()),
        ("trip_update.trip.route_id", pyarrow.large_string()),
        ("trip_update.trip.direction_id", pyarrow.uint8()),
        ("trip_update.trip.start_time", pyarrow.large_string()),
        ("trip_update.trip.start_date", pyarrow.large_string()),
        ("trip_update.trip.schedule_relationship", pyarrow.large_string()),
        ("trip_update.trip.route_pattern_id", pyarrow.large_string()),
        ("trip_update.trip.tm_trip_id", pyarrow.large_string()),
        ("trip_update.trip.overload_id", pyarrow.int64()),
        ("trip_update.trip.overload_offset", pyarrow.int64()),
        ("trip_update.trip.revenue", pyarrow.bool_()),
        ("trip_update.trip.last_trip", pyarrow.bool_()),
        ("trip_update.vehicle.id", pyarrow.large_string()),
        ("trip_update.vehicle.label", pyarrow.large_string()),
        ("trip_update.vehicle.license_plate", pyarrow.large_string()),
        # trip_update.vehicle.consist: list<element: struct<label: string>>
        #   child 0, element: struct<label: string>
        #       child 0, label: string
        ("trip_update.vehicle.assignment_status", pyarrow.large_string()),
        ("trip_update.timestamp", pyarrow.timestamp("us")),
        ("trip_update.delay", pyarrow.int32()),
        ("feed_timestamp", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.stop_sequence", pyarrow.uint32()),
        ("trip_update.stop_time_update.stop_id", pyarrow.large_string()),
        ("trip_update.stop_time_update.arrival.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.arrival.time", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.arrival.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.time", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.departure.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.schedule_relationship", pyarrow.large_string()),
        ("trip_update.stop_time_update.boarding_status", pyarrow.large_string()),
    ]
)


def lrtp_prod(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    terminal_stop_ids = list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))

    polars_df = polars_df.filter(pl.col("trip_update.stop_time_update.stop_id").is_in(terminal_stop_ids))
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


def lrtp_devgreen(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    terminal_stop_ids = list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))

    polars_df = polars_df.filter(
        ~pl.col("trip_update.stop_time_update.departure.time").is_null()
        & pl.col("trip_update.stop_time_update.stop_id").is_in(terminal_stop_ids)
    )
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


def heavyrail(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    terminal_stop_ids = list(map(str, [70001, 70036, 70038, 70059, 70061, 70094, 70105]))
    polars_df = polars_df.filter(
        ~pl.col("trip_update.stop_time_update.departure.time").is_null()
        & pl.col("trip_update.stop_time_update.stop_id").is_in(terminal_stop_ids)
    )
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


def apply_timezone_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply timezone conversions to lamp data before outputting for tableau consumption
    """
    polars_df = polars_df.with_columns(
        pl.from_epoch(pl.col("trip_update.stop_time_update.departure.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.stop_time_update.arrival.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
    )
    return polars_df


def schema() -> pyarrow.schema:
    """
    Function returns schema of the processed gtfs-rt trip updates
    """
    return gtfs_rt_trip_updates_processed_schema
