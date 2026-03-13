from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dataframely as dy
import polars as pl

from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions
from lamp_py.runtime_utils.process_logger import ProcessLogger


class StopEvents(dy.Schema):
    """Actual departures and arrivals by trip, route, vehicle, and stop sequence."""

    id = dy.String()  # date-trip-route-vehicle-stop_sequence
    timestamp = dy.Int64()
    start_date = dy.String(primary_key=True)
    route_id = dy.String(primary_key=True)
    trip_id = dy.String(primary_key=True)
    vehicle_id = dy.String(primary_key=True)
    stop_sequence = dy.Int64(primary_key=True)
    direction_id = dy.Int64(min=0, max=1)
    start_time = dy.String()
    revenue = dy.Bool()
    stop_id = dy.String()
    arrived = dy.Int64(nullable=True)
    departed = dy.Int64(nullable=True)
    latest_stopped_timestamp = dy.Int64(nullable=True)


def unnest_vehicle_positions(vp: dy.DataFrame[VehiclePositions]) -> dy.DataFrame[StopEvents]:
    """Unnest VehiclePositions data into flat table."""
    process_logger = ProcessLogger("unnest_vehicle_positions", input_rows=vp.height)
    process_logger.log_start()
    events = (
        vp.select("entity")
        .explode("entity")
        .unnest("entity")
        .unnest("vehicle")
        .unnest("trip")
        .rename({"id": "vehicle_id", "current_stop_sequence": "stop_sequence"})
        .filter(
            pl.col("stop_sequence").is_not_null(),
            pl.col("trip_id").is_not_null(),
            pl.col("timestamp").is_not_null(),
        )
        .select(
            pl.concat_str(StopEvents.primary_key(), separator="-").alias("id"),
            "timestamp",
            "start_date",
            "trip_id",
            "vehicle_id",
            "stop_sequence",
            "direction_id",
            "route_id",
            "start_time",
            "revenue",
            "stop_id",
            pl.when(pl.col("current_status").eq("STOPPED_AT")).then(pl.col("timestamp")).alias("arrived"),
            pl.lit(None).alias("departed"),  # for schema adherence
            pl.when(pl.col("current_status").eq("STOPPED_AT"))
            .then(pl.col("timestamp"))
            .alias("latest_stopped_timestamp"),
        )
    )

    valid = process_logger.log_dataframely_filter_results(*StopEvents.filter(events, cast=True))

    process_logger.log_complete()

    return valid


def update_records(
    existing_records: dy.DataFrame[StopEvents],
    new_records: dy.DataFrame[StopEvents],
    max_record_age: timedelta,
) -> dy.DataFrame[StopEvents]:
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
        .join(new_records, on=StopEvents.primary_key(), how="full", coalesce=True)
        .select(
            *StopEvents.primary_key(),
            *[
                pl.coalesce(col, f"{col}_right").alias(col)
                for col in [
                    "id",
                    "direction_id",
                    "start_time",
                    "revenue",
                    "stop_id",
                    "arrived",
                ]
            ],
            pl.coalesce(
                pl.when(  # if the trip has moved past this stop sequence, set departed to latest_stopped_timestamp
                    pl.col("stop_sequence")
                    .max()
                    .over(["start_date", "route_id", "trip_id", "vehicle_id"])
                    .gt(pl.col("stop_sequence"))
                ).then(pl.col("latest_stopped_timestamp")),
                "departed",
            ).alias("departed"),
            pl.coalesce(
                pl.when(  # if departure is updated, then also update timestamp
                    pl.col("stop_sequence")
                    .max()
                    .over(["start_date", "route_id", "trip_id", "vehicle_id"])
                    .gt(pl.col("stop_sequence")),
                    pl.col("departed").is_null(),
                ).then(pl.col("timestamp_right").max().over(["start_date", "route_id", "trip_id", "vehicle_id"])),
                "timestamp",
                "timestamp_right",
            ).alias("timestamp"),
            pl.coalesce("latest_stopped_timestamp_right", "latest_stopped_timestamp").alias(
                "latest_stopped_timestamp"
            ),  # use value from new record
        )
        .filter(pl.col("arrived").is_not_null() | pl.col("departed").is_not_null())  # keep only stops with events
    )

    valid = process_logger.log_dataframely_filter_results(*StopEvents.filter(combined, cast=True))

    process_logger.add_metadata(new_records=new_records.height, updated_records=combined.height)

    process_logger.log_complete()

    return valid
