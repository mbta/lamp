from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dataframely as dy
import polars as pl

from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions, VehiclePositionsApiFormat
from lamp_py.runtime_utils.process_logger import ProcessLogger


class VehicleEvents(dy.Schema):
    """Vehicle Position raw events to be de-duplicated into actual events"""

    id = dy.String(primary_key=True)  # start_date-trip-route-vehicle
    timestamp = dy.Int64()
    start_date = dy.String(nullable=False)
    trip_id = dy.String()
    direction_id = dy.Int8(min=0, max=1, nullable=True)
    route_id = dy.String()
    start_time = dy.String(nullable=True)
    revenue = dy.Bool(nullable=True)
    stop_id = dy.String(nullable=False)
    current_stop_sequence = dy.Int16(primary_key=True)
    current_status = dy.String(nullable=True)
    status_start_timestamp = dy.Int64(nullable=True)
    status_end_timestamp = dy.Int64(nullable=True)


class VehicleStopEvents(dy.Schema):
    """Vehicle Position raw events to be de-duplicated into actual events"""

    id = dy.String(primary_key=True)  # start_date-trip-route-vehicle
    timestamp = dy.Int64()
    start_date = dy.String(nullable=False)
    trip_id = VehicleEvents.trip_id
    direction_id = VehicleEvents.direction_id
    route_id = VehicleEvents.route_id
    start_time = VehicleEvents.start_time
    revenue = VehicleEvents.revenue
    stop_id = VehicleEvents.stop_id
    current_stop_sequence = VehicleEvents.current_stop_sequence
    # remove current status
    # renamed status start and stop to arrival and departure for stop events schema
    arrived = VehicleEvents.status_start_timestamp
    departed = VehicleEvents.status_end_timestamp


class StopEventsJSON(dy.Schema):
    """Pre-serialized stop events for trips."""

    id = dy.String(primary_key=True)
    timestamp = dy.Int64()
    start_date = VehicleStopEvents.start_date
    trip_id = VehicleStopEvents.trip_id
    direction_id = VehicleStopEvents.direction_id
    route_id = VehicleStopEvents.route_id
    start_time = VehicleStopEvents.start_time
    revenue = VehicleStopEvents.revenue
    stop_events = dy.List(
        dy.Struct(
            inner={
                "stop_id": VehicleStopEvents.stop_id,
                "current_stop_sequence": VehicleStopEvents.current_stop_sequence,
                "arrived": VehicleStopEvents.arrived,
                "departed": VehicleStopEvents.departed,
            }
        )
    )


def unnest_vehicle_positions(vp: dy.DataFrame[VehiclePositionsApiFormat]) -> dy.DataFrame[VehiclePositions]:
    """Unnest VehiclePositions data into flat table."""
    process_logger = ProcessLogger("unnest_vehicle_positions", input_rows=vp.height)
    process_logger.log_start()
    vehicle_positions = vp.select("entity").explode("entity").unnest("entity").unnest("vehicle").unnest("trip")

    valid = process_logger.log_dataframely_filter_results(*VehiclePositions.filter(vehicle_positions, cast=True))

    process_logger.log_complete()

    return valid


def vehicle_position_to_archive_events(vp: dy.DataFrame[VehiclePositions]) -> dy.DataFrame[VehicleEvents]:
    """
    Convert VehiclePositions data into VehicleEvents format.

    Filters vehicle position records to include only those with valid stop sequences,
    trip IDs, timestamps, and route IDs. Generates a composite ID from start_date,
    trip_id, route_id, and vehicle id, then selects relevant columns for event archival.

    Start_date is required to have a unique identifier across days, as all other identifiers are reusable.
    """
    process_logger = ProcessLogger("vehicle_position_to_archive_events", input_rows=vp.height)
    process_logger.log_start()
    events = (
        vp.filter(
            pl.col("current_stop_sequence").is_not_null(),
            pl.col("trip_id").is_not_null(),
            pl.col("timestamp").is_not_null(),
            pl.col("route_id").is_not_null(),
            pl.col("start_date").is_not_null(),
        )
        .select(
            pl.concat_str(
                pl.col("start_date"), pl.col("trip_id"), pl.col("route_id"), pl.col("id"), separator="-"
            ).alias("id"),
            "timestamp",
            "start_date",
            "trip_id",
            "direction_id",
            "route_id",
            "start_time",
            "revenue",
            "stop_id",
            "current_stop_sequence",
            "current_status",
        )
        .with_columns(
            pl.lit(None).cast(pl.Int64).alias("status_start_timestamp"),
            pl.lit(None).cast(pl.Int64).alias("status_end_timestamp"),
        )
    )

    valid = process_logger.log_dataframely_filter_results(*VehicleEvents.filter(events, cast=True))

    process_logger.log_complete()

    return valid


def aggregate_duration_with_new_records(
    existing_records: dy.DataFrame[VehicleStopEvents],
    new_records: dy.DataFrame[VehicleEvents],
) -> dy.DataFrame[VehicleEvents]:
    """
    Recalculate derived duration fields for stop events based on status changes.

    Merges existing and new stop event records, groups them by vehicle ID and stop
    sequence, and calculates the timestamp when each status began and ended. Returns
    only records that pass StopEventsWithStatus validation.

    Args:
        existing_records: DataFrame of previously processed stop events with status information.
        new_records: DataFrame of newly received stop events with status information.
        max_record_age: Maximum age threshold for records (currently logged but not actively used in filtering).

    Returns:
        DataFrame of stop events with validated derived duration fields (status_start_timestamp
        and status_end_timestamp where applicable).

    Note:
        Records are sorted by timestamp and grouped by vehicle ID, stop sequence, and current status.
        Status end timestamp is only set when the first and last timestamp within a group differ.
    """
    process_logger = ProcessLogger(
        "aggregate_duration_with_new_records",
        existing_records=existing_records.height,
    )
    process_logger.log_start()
    all_events = pl.concat([existing_records, new_records], how="diagonal")

    combined = (
        all_events.sort(by="timestamp")
        .group_by("id", "current_stop_sequence", "current_status")
        .agg(
            [
                pl.first("timestamp").alias("status_start_timestamp"),
                pl.when(pl.first("timestamp").ne(pl.last("timestamp"))).then(
                    pl.last("timestamp").alias("status_end_timestamp")
                ),
            ]
        )
    ).join(
        all_events.select(
            ["id", "direction_id", "revenue", "route_id", "start_date", "start_time", "stop_id", "timestamp", "trip_id"]
        ),
        on="id",
        how="left",
    )
    valid = process_logger.log_dataframely_filter_results(*VehicleEvents.filter(combined, cast=True))

    process_logger.add_metadata(new_records=new_records.height, updated_records=combined.height)

    process_logger.log_complete()

    return valid


def filter_stop_events(
    compressed_events: dy.DataFrame[VehicleEvents],
    max_record_age: timedelta,
) -> dy.DataFrame[VehicleStopEvents]:
    """
    take compressed events and take only stopped_at events,
    and rename the status start and end periods to stop event schema format
    """

    filtered = (
        compressed_events.filter(
            (pl.col("current_status") == "STOPPED_AT")
            & (pl.col("status_start_timestamp").is_not_null() | pl.col("status_end_timestamp").is_not_null())
            & (
                datetime.now(tz=ZoneInfo("America/New_York"))
                - pl.from_epoch("timestamp").dt.replace_time_zone(
                    "America/New_York", ambiguous="latest", non_existent="null"
                )
                < max_record_age
            )  # remove records that are older than max_record_age - flashback usecase only requires max_record_age history
        )
        .drop("current_status")
        .sort("id", "current_stop_sequence")
        .rename({"status_start_timestamp": "arrived", "status_end_timestamp": "departed"})
    )

    valid = ProcessLogger("filter_stop_events").log_dataframely_filter_results(
        *VehicleStopEvents.filter(filtered, cast=True)
    )

    return valid


def structure_stop_events(df: dy.DataFrame[VehicleStopEvents]) -> dy.DataFrame[StopEventsJSON]:
    """Structure flat table into StopEvents records."""
    process_logger = ProcessLogger("structure_stop_events", input_rows=df.height)
    stop_events = df.group_by("id").agg(
        pl.max("timestamp").alias("timestamp"),
        pl.selectors.by_name("start_date", "trip_id", "direction_id", "route_id", "start_time", "revenue").first(),
        pl.struct("stop_id", "current_stop_sequence", "arrived", "departed").alias("stop_events"),
    )

    valid = process_logger.log_dataframely_filter_results(*StopEventsJSON.filter(stop_events, cast=True))

    process_logger.log_complete()

    return valid
