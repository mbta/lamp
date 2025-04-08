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
        ("trip_update.timestamp", pyarrow.timestamp("ms")),
        ("trip_update.delay", pyarrow.int32()),
        ("feed_timestamp", pyarrow.timestamp("ms")),
        ("trip_update.stop_time_update.stop_sequence", pyarrow.uint32()),
        ("trip_update.stop_time_update.stop_id", pyarrow.large_string()),
        ("trip_update.stop_time_update.arrival.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.arrival.time", pyarrow.timestamp("ms")),
        ("trip_update.stop_time_update.arrival.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.time", pyarrow.timestamp("ms")),
        ("trip_update.stop_time_update.departure.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.schedule_relationship", pyarrow.large_string()),
        ("trip_update.stop_time_update.boarding_status", pyarrow.large_string()),
    ]
)


def apply_gtfs_rt_trip_updates_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    polars_df = polars_df.with_columns(
        pl.col("trip_update.stop_time_update.departure.time")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.col("trip_update.stop_time_update.arrival.time")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.col("trip_update.timestamp")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.col("feed_timestamp")
        .str.strptime(pl.Datetime("ms"), "%Y-%m-%dT%H:%M:%SZ", strict=False)
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
    )
    return polars_df


def schema() -> pyarrow.schema:
    """
    Function returns schema of the processed gtfs-rt trip updates
    """
    return gtfs_rt_trip_updates_processed_schema
