from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dataframely as dy
import polars as pl

from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions, VehiclePositionsApiFormat
from lamp_py.runtime_utils.process_logger import ProcessLogger


class StopEventsWithStatus(dy.Schema):
    """Flat events data, with additional information for determining stop departures."""

    id = dy.String(primary_key=True)  # trip-route-vehicle
    timestamp = dy.Int64()
    start_date = dy.String(nullable=True)
    trip_id = dy.String()
    direction_id = dy.Int8(min=0, max=1, nullable=True)
    route_id = dy.String()
    start_time = dy.String(nullable=True)
    revenue = dy.Bool(nullable=True)
    stop_id = dy.String(nullable=False)
    current_stop_sequence = dy.Int16(primary_key=True)
    current_status = dy.String(nullable=True)


class StopEventsTable(dy.Schema):
    """Flat events data, with additional information for determining stop departures."""

    id = dy.String(primary_key=True)  # trip-route-vehicle
    timestamp = dy.Int64()
    start_date = dy.String(nullable=True)
    trip_id = dy.String()
    direction_id = dy.Int8(min=0, max=1, nullable=True)
    route_id = dy.String()
    start_time = dy.String(nullable=True)
    revenue = dy.Bool(nullable=True)
    stop_id = dy.String(nullable=False)
    current_stop_sequence = dy.Int16(primary_key=True)
    arrived = dy.Int64(nullable=True)
    departed = dy.Int64(nullable=True)
    latest_stopped_timestamp = dy.Int64(nullable=True)


class StopEventsJSON(dy.Schema):
    """Pre-serialized stop events for trips."""

    id = dy.String(primary_key=True)
    timestamp = dy.Int64()
    start_date = StopEventsTable.start_date
    trip_id = StopEventsTable.trip_id
    direction_id = StopEventsTable.direction_id
    route_id = StopEventsTable.route_id
    start_time = StopEventsTable.start_time
    revenue = StopEventsTable.revenue
    stop_events = dy.List(
        dy.Struct(
            inner={
                "stop_id": StopEventsTable.stop_id,
                "current_stop_sequence": dy.Int16(),
                "arrived": StopEventsTable.arrived,
                "departed": StopEventsTable.departed,
            }
        )
    )


def unnest_vehicle_positions(vp: dy.DataFrame[VehiclePositions]) -> dy.DataFrame[StopEventsTable]:
    """Unnest VehiclePositions data into flat table."""
    process_logger = ProcessLogger("unnest_vehicle_positions", input_rows=vp.height)
    process_logger.log_start()
    events = (
        vp.select("entity")
        .explode("entity")
        .unnest("entity")
        .unnest("vehicle")
        .unnest("trip")
        .filter(
            pl.col("current_stop_sequence").is_not_null(),
            pl.col("trip_id").is_not_null(),
            pl.col("timestamp").is_not_null(),
            pl.col("route_id").is_not_null(),
        )
        .select(
            pl.concat_str(pl.col("trip_id"), pl.col("route_id"), pl.col("id"), separator="-").alias("id"),
            "timestamp",
            "start_date",
            "trip_id",
            "direction_id",
            "route_id",
            "start_time",
            "revenue",
            "stop_id",
            "current_stop_sequence",
            pl.when(pl.col("current_status").eq("STOPPED_AT")).then(pl.col("timestamp")).alias("arrived"),
            pl.lit(None).alias("departed"),  # for schema adherence
            pl.when(pl.col("current_status").eq("STOPPED_AT"))
            .then(pl.col("timestamp"))
            .alias("latest_stopped_timestamp"),
        )
    )

    valid = process_logger.log_dataframely_filter_results(*StopEventsTable.filter(events, cast=True))

    process_logger.log_complete()

    return valid


def unnest_vehicle_positions_new(vp: dy.DataFrame[VehiclePositionsApiFormat]) -> dy.DataFrame[StopEventsTable]:
    """Unnest VehiclePositions data into flat table."""
    process_logger = ProcessLogger("unnest_vehicle_positions", input_rows=vp.height)
    process_logger.log_start()
    vehicle_positions = vp.select("entity").explode("entity").unnest("entity").unnest("vehicle").unnest("trip")
    process_logger.log_complete()

    return vehicle_positions


def vehicle_position_to_raw_events(vp: pl.DataFrame) -> pl.DataFrame:
    """
    Convert VehiclePositions data into StopEventsTable format,
    extracting stop events based on current status and stop sequence.
    """
    process_logger = ProcessLogger("unnest_vehicle_positions", input_rows=vp.height)
    process_logger.log_start()
    events = vp.filter(
        pl.col("current_stop_sequence").is_not_null(),
        pl.col("trip_id").is_not_null(),
        pl.col("timestamp").is_not_null(),
        pl.col("route_id").is_not_null(),
    ).select(
        pl.concat_str(pl.col("trip_id"), pl.col("route_id"), pl.col("id"), separator="-").alias("id"),
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

    return events


def aggregate_duration_with_new_records(
    existing_records: pl.DataFrame,
    new_records: pl.DataFrame,
) -> pl.DataFrame:
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

    process_logger.add_metadata(new_records=new_records.height, updated_records=combined.height)

    process_logger.log_complete()

    return combined


def filter_stop_events(
    compressed_events: pl.DataFrame,
    max_record_age: timedelta,
) -> dy.DataFrame[StopEventsTable]:
    """
    take compressed events and take only stopped_at events,
    and rename the status start and end periods to stop event schema format
    """

    return (
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


def update_records(
    existing_records: dy.DataFrame[StopEventsTable],
    new_records: dy.DataFrame[StopEventsTable],
    max_record_age: timedelta,
) -> dy.DataFrame[StopEventsTable]:
    """Return a DataFrame of recent stops using VehiclePositions."""
    process_logger = ProcessLogger(
        "update_records", existing_records=existing_records.height, max_record_age=str(max_record_age)
    )
    process_logger.log_start()

    combined = (
        existing_records.filter(  # remove old records
            datetime.now(tz=ZoneInfo("America/New_York"))
            - pl.from_epoch("timestamp").dt.replace_time_zone(
                "America/New_York", ambiguous="latest", non_existent="null"
            )
            < max_record_age
        )
        .join(new_records, on=["id", "current_stop_sequence"], how="full", coalesce=True)
        .select(
            "id",
            "current_stop_sequence",
            *[
                pl.coalesce(col, f"{col}_right").alias(col)
                for col in [
                    "start_date",
                    "trip_id",
                    "direction_id",
                    "route_id",
                    "start_time",
                    "revenue",
                    "stop_id",
                    "arrived",
                ]
            ],
            pl.coalesce(
                pl.when(  # if the trip has moved past this stop sequence, set departed to latest_stopped_timestamp
                    pl.col("current_stop_sequence").max().over("id").gt(pl.col("current_stop_sequence"))
                ).then(pl.col("latest_stopped_timestamp")),
                "departed",
            ).alias("departed"),
            pl.coalesce(
                pl.when(  # if departure is updated, then also update timestamp
                    pl.col("current_stop_sequence").max().over("id").gt(pl.col("current_stop_sequence")),
                    pl.col("departed").is_null(),
                ).then(pl.col("timestamp_right").max().over("id")),
                "timestamp",
                "timestamp_right",
            ).alias("timestamp"),
            pl.coalesce("latest_stopped_timestamp_right", "latest_stopped_timestamp").alias(
                "latest_stopped_timestamp"
            ),  # use value from new record
        )
        .filter(pl.col("arrived").is_not_null() | pl.col("departed").is_not_null())  # keep only stops with events
        # maybe join schedule in to see when arrived is valid.
    )

    valid = process_logger.log_dataframely_filter_results(*StopEventsTable.filter(combined, cast=True))

    # TODO : mark last stop sequence as arived if max timestamp is incoming at terminus

    process_logger.add_metadata(new_records=new_records.height, updated_records=combined.height)

    process_logger.log_complete()

    return valid


def structure_stop_events(df: dy.DataFrame[StopEventsTable]) -> dy.DataFrame[StopEventsJSON]:
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
