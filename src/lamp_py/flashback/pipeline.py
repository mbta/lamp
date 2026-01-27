import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dataframely as dy
import polars as pl
from aiohttp import ClientSession

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3Location

stop_events_location = S3Location(
    bucket=S3_ARCHIVE,
    prefix=f"{LAMP}/ad_hoc/stop_events.json",
    version="0.1.0",
)


class StopEventsTable(dy.Schema):
    """Flat events data, with additional information for determining stop departures."""

    id = dy.String(primary_key=True)
    timestamp = dy.Int64()
    start_date = dy.String()
    trip_id = dy.String()
    direction_id = dy.Int8(min=0, max=1)
    route_id = dy.String()
    start_time = dy.String()
    revenue = dy.Bool()
    stop_id = dy.String()
    current_stop_sequence = dy.Int16(primary_key=True)
    arrived = dy.Int64(nullable=True)
    departed = dy.Int64(nullable=True)
    latest_stopped_timestamp = dy.Int64(nullable=True)


class StopEventsJSON(dy.Schema):
    """Pre-serialized stop events for trips."""

    id = dy.String(primary_key=True)
    timestamp = dy.Int64()
    trip = dy.Struct(
        inner={
            "start_date": StopEventsTable.start_date,
            "trip_id": StopEventsTable.trip_id,
            "direction_id": StopEventsTable.direction_id,
            "route_id": StopEventsTable.route_id,
            "start_time": StopEventsTable.start_time,
            "revenue": StopEventsTable.revenue,
        },
    )
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


def unnest_vehicle_positions(df: pl.LazyFrame) -> dy.DataFrame[StopEventsTable]:
    """Unnest VehiclePositions data into flat table."""
    process_logger = ProcessLogger("unnest_vehicle_positions")
    events = StopEventsTable.cast(
        df.select("entity")
        .explode("entity")
        .unnest("entity")
        .unnest("vehicle")
        .unnest("trip")
        .filter(pl.col("current_stop_sequence").is_not_null())
        .select(
            pl.concat_str(pl.col("trip_id"), pl.lit("-"), pl.col("id")).alias("id"),
            "timestamp",
            "start_date",
            "trip_id",
            pl.col("direction_id"),
            "route_id",
            "start_time",
            "revenue",
            "stop_id",
            pl.col("current_stop_sequence"),
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


async def get_vehicle_positions(
    url: str = "https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json",
) -> pl.LazyFrame:
    """Fetch the latest VehiclePositions data."""
    process_logger = ProcessLogger("get_vehicle_positions", url=url)
    process_logger.log_start()
    async with ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()

    # TODO : handle fetch errors

    vehicle_positions = pl.scan_ndjson(data)

    process_logger.log_complete()

    return vehicle_positions


def update_records(
    existing_records: dy.DataFrame[StopEventsTable],
    new_records: dy.DataFrame[StopEventsTable],
    max_record_age: timedelta = timedelta(hours=2),
) -> dy.DataFrame[StopEventsTable]:
    """Return a DataFrame of recent stops using VehiclePositions."""
    process_logger = ProcessLogger(
        "update_records", existing_records=existing_records.height, max_record_age=str(max_record_age)
    )
    process_logger.log_start()

    combined = (
        existing_records.filter(  # remove old records
            datetime.now(tz=ZoneInfo("America/New_York"))
            - pl.from_epoch("timestamp").dt.replace_time_zone("America/New_York")
            < max_record_age
        )
        .join(new_records, on=["id", "current_stop_sequence"], how="full", coalesce=True)
        .select(
            "id",
            "current_stop_sequence",
            *[
                pl.coalesce(col, f"{col}_right").alias(col)
                for col in [
                    "timestamp",
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
            pl.coalesce("latest_stopped_timestamp_right", "latest_stopped_timestamp").alias(
                "latest_stopped_timestamp"
            ),  # use value from new record
        )
        .filter(pl.col("arrived").is_not_null() | pl.col("departed").is_not_null())  # keep only stops with events
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
        pl.struct("start_date", "trip_id", "direction_id", "route_id", "start_time", "revenue").first().alias("trip"),
        pl.struct("stop_id", "current_stop_sequence", "arrived", "departed").alias("stop_events"),
    )

    valid = process_logger.log_dataframely_filter_results(*StopEventsJSON.filter(stop_events, cast=True))

    process_logger.log_complete()

    return valid


def get_remote_events(location: S3Location = stop_events_location) -> dy.DataFrame[StopEventsTable]:
    """Fetch existing stop events from S3."""
    process_logger = ProcessLogger("get_remote_events")
    process_logger.log_start()
    try:
        remote_events = process_logger.log_dataframely_filter_results(
            *StopEventsJSON.filter(pl.scan_ndjson(location.s3_uri), cast=True)
        )

        existing_events = StopEventsTable.cast(
            pl.concat(
                [
                    StopEventsTable.create_empty(),
                    remote_events.unnest("trip").explode("stop_events").unnest("stop_events"),
                ],
                how="diagonal",
            )
        )

    except OSError as e:
        process_logger.log_warning(e)
        existing_events = StopEventsTable.create_empty()

    process_logger.log_complete()

    return existing_events


async def flashback(
    existing_events: dy.DataFrame[StopEventsTable], max_record_age: timedelta = timedelta(hours=2)
) -> None:
    """Fetch, process, and store stop events."""
    existing_events = existing_events
    while True:
        process_logger = ProcessLogger("flashback")
        process_logger.log_start()
        new_records = await get_vehicle_positions()

        stop_events = update_records(existing_events, unnest_vehicle_positions(new_records), max_record_age)

        existing_events = stop_events

        structure_stop_events(stop_events).write_ndjson(stop_events_location.s3_uri)

        process_logger.log_complete()

        await asyncio.sleep(5)  # wait before fetching new data


def pipeline() -> None:
    """Entry point for flashback stop events pipeline."""
    process_logger = ProcessLogger("main")
    process_logger.log_start()

    asyncio.run(flashback(get_remote_events()))
