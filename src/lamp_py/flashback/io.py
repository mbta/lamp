from time import sleep

import dataframely as dy
import polars as pl
from aiohttp import ClientError, ClientSession

from lamp_py.flashback.events import (
    StopEventsJSON,
    VehicleStopEvents,
    unnest_vehicle_positions,
)
from lamp_py.ingestion.convert_gtfs_rt import VehiclePositions, VehiclePositionsApiFormat
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3Location
from lamp_py.runtime_utils.remote_files import stop_events as stop_events_location


def get_remote_events(location: S3Location = stop_events_location) -> dy.DataFrame[VehicleStopEvents]:
    """Fetch existing stop events from S3."""
    process_logger = ProcessLogger("get_remote_events")
    process_logger.log_start()
    try:
        remote_events = process_logger.log_dataframely_filter_results(
            *StopEventsJSON.filter(pl.scan_parquet(location.s3_uri), cast=True)
        )

        # TODO : read in vehicle_positions parquet when available?

        existing_events = VehicleStopEvents.cast(
            pl.concat(
                [
                    VehicleStopEvents.create_empty(),
                    remote_events.explode("stop_events").unnest("stop_events"),
                ],
                how="diagonal",
            )
        )

    except OSError as e:
        process_logger.log_warning(e)
        existing_events = VehicleStopEvents.create_empty()

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

    vehicle_positions = pl.read_ndjson(data, schema=VehiclePositionsApiFormat.to_polars_schema())
    valid = process_logger.log_dataframely_filter_results(*VehiclePositionsApiFormat.filter(vehicle_positions))

    process_logger.log_complete()

    return unnest_vehicle_positions(valid)
