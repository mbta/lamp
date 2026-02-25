import asyncio
from datetime import timedelta
from os import environ
from signal import SIGTERM, signal

import dataframely as dy

from lamp_py.aws.ecs import handle_ecs_sigterm
from lamp_py.flashback.events import (
    VehicleStopEvents,
    filter_stop_events,
    structure_stop_events,
    aggregate_duration_with_new_records,
    vehicle_position_to_archive_events,
)
from lamp_py.flashback.io import get_remote_events, get_vehicle_positions
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import stop_events as stop_events_location


async def flashback(
    remote_events: dy.DataFrame[VehicleStopEvents],
    max_record_age: timedelta = timedelta(hours=2),
    local_override: str | None = None,
) -> None:
    """Fetch, process, and store stop events."""
    all_events = remote_events

    while True:
        process_logger = ProcessLogger("flashback")
        process_logger.log_start()

        # vehicle positions flattened, entire message
        new_records = await get_vehicle_positions()

        # vehicle positions validated and filtered down to columns of interest - i.e. removed lat/lon, occupancy
        # i.e. vehicle reporting STOPPED_AT at time timestamp
        new_events = vehicle_position_to_archive_events(new_records)

        # consolidate records with same stop status and sequence - generate start/stop time for each status type
        # single record for each event type, with new fields indicating duration of the event.
        # i.e. vehicle STOPPED_AT for status_start_timestamp to status_end_timestamp.
        compressed_events = aggregate_duration_with_new_records(all_events, new_events)

        # generate flashback events for from stop records
        # for flashback, only care about STOPPED_AT events - filter on those, and prepare structure for json export
        compressed_stop_events = filter_stop_events(compressed_events, max_record_age)

        output_path = local_override or stop_events_location.s3_uri
        process_logger.add_metadata(write_path=output_path)

        # convert to agreed upon json structure, and export
        await asyncio.to_thread(lambda: structure_stop_events(compressed_stop_events).write_parquet(output_path))

        process_logger.log_complete()

        await asyncio.sleep(3)  # wait before fetching new data


def pipeline(local_override: str | None = None) -> None:
    """Entry point for flashback stop events pipeline."""
    process_logger = ProcessLogger("main")
    process_logger.log_start()

    signal(SIGTERM, handle_ecs_sigterm)

    # configure the environment
    environ["SERVICE_NAME"] = "flashback_event_service"

    validate_environment(
        required_variables=[
            "ARCHIVE_BUCKET",
        ],
    )

    asyncio.run(flashback(get_remote_events(), local_override=local_override))
