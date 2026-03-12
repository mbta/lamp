from logging import ERROR
from time import sleep

import dataframely as dy
import polars as pl
from aiohttp import ClientError, ClientSession

from lamp_py.flashback.events import StopEvents
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3Location
from lamp_py.runtime_utils.remote_files import stop_events as stop_events_location


def get_remote_events(location: S3Location = stop_events_location) -> dy.DataFrame[StopEvents]:
    """Fetch existing stop events from S3."""
    process_logger = ProcessLogger("get_remote_events")
    process_logger.log_start()
    existing_events = StopEvents.create_empty()
    try:
        remote_events = pl.read_ndjson(location.s3_uri, schema=StopEvents.to_polars_schema())
    except OSError as e:
        process_logger.log_warning(e)
        remote_events = StopEvents.create_empty()

    try:
        remote_events = process_logger.log_dataframely_filter_results(
            *StopEvents.filter(remote_events, cast=True), log_level=ERROR
        )

        existing_events = StopEvents.cast(
            pl.concat(
                [
                    existing_events,
                    remote_events,
                ],
                how="vertical",
            )
        )

    except dy.exc.SchemaError as e:
        process_logger.log_failure(e)

    process_logger.log_complete()

    return existing_events


async def get_vehicle_positions(
    url: str = "https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json",
    sleep_interval: int = 3,
    max_retries: int = 10,
) -> dy.DataFrame[VehiclePositions]:
    """Fetch the latest VehiclePositions data."""
    process_logger = ProcessLogger("get_vehicle_positions", url=url)
    process_logger.log_start()

    async with ClientSession() as session:
        for attempt in range(max_retries + 1):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.read()
                    break
            except ClientError as e:
                process_logger.log_failure(e)
                if attempt == max_retries:
                    raise ClientError(f"Maximum retries ({max_retries}) exceeded") from e
                sleep(sleep_interval)

    vehicle_positions = pl.read_ndjson(data, schema=VehiclePositions.to_polars_schema())

    valid = process_logger.log_dataframely_filter_results(*VehiclePositions.filter(vehicle_positions))

    process_logger.log_complete()

    return valid


def write_stop_events(stop_events: dy.DataFrame[StopEvents], location: S3Location = stop_events_location) -> None:
    """Write stop events to specified location."""
    process_logger = ProcessLogger("write_stop_events", s3_uri=location.s3_uri)
    process_logger.log_start()
    (
        stop_events
        .sort(["start_time", "route_id", "stop_sequence"]) # for convenience—--and maybe compression?
        .write_ndjson(location.s3_uri, compression="gzip", compression_level=9)
    )
    process_logger.log_complete()
