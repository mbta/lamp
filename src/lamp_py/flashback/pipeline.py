import asyncio
from datetime import timedelta
from os import environ
from signal import SIGTERM, signal

import dataframely as dy

from lamp_py.aws.ecs import handle_ecs_sigterm
from lamp_py.flashback.events import (
    VehicleEvents,
    filter_stop_events,
    structure_stop_events,
    aggregate_duration_with_new_records,
    vehicle_position_to_archive_events,
)
from lamp_py.flashback.io import get_remote_all_events, get_vehicle_positions
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import stop_events, vehicle_position_all_events, stop_events_json


async def flashback(
    remote_events: dy.DataFrame[VehicleEvents],
    max_record_age: timedelta = timedelta(hours=2),
    local_override_path: str | None = None,
) -> None:
    """Fetch, process, and store stop events."""
    all_events = remote_events

    # vehicle_events - how to handle in transit events? filter out same timestamp, or aggregate somehow?
    # average speed?

    # do i want to keep everything? hmm..

    while True:
        process_logger = ProcessLogger("flashback")
        process_logger.log_start()

        # raw, flat vehicle position
        new_records = await get_vehicle_positions()

        # add event_id, event duration columns
        new_events = vehicle_position_to_archive_events(new_records)

        # combine and update events
        compressed_events = aggregate_duration_with_new_records(all_events, new_events)

        # update all_events with the newly compressed events
        all_events = all_events.update(  # type: ignore[assignment]
            compressed_events, on=["event_id", "current_stop_sequence", "current_status"], how="full"
        )

        # take only meaningful stop events for flashback
        compressed_stop_events = filter_stop_events(compressed_events, max_record_age)

        process_logger.add_metadata(
            new_records=new_records.height,
            compressed_events=compressed_events.height,
            compressed_stop_events=compressed_stop_events.height,
        )

        if local_override_path:
            stop_events_uri = f"{local_override_path}/stop_events.parquet"
            stop_events_json_uri = f"{local_override_path}/stop_events.ndjson"
            all_events_uri = f"{local_override_path}/vehicle_position_all_events.parquet"
        else:
            stop_events_uri = stop_events.s3_uri
            stop_events_json_uri = stop_events_json.s3_uri
            all_events_uri = vehicle_position_all_events.s3_uri

        await asyncio.to_thread(lambda: structure_stop_events(compressed_stop_events).write_parquet(stop_events_uri))
        await asyncio.to_thread(
            lambda: structure_stop_events(compressed_stop_events).write_ndjson(stop_events_json_uri)
        )

        await asyncio.to_thread(lambda: all_events.write_parquet(all_events_uri))

        process_logger.log_complete()

        await asyncio.sleep(3)  # wait before fetching new data

def pipeline(local_override_path: str | None = None) -> None:
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

    asyncio.run(flashback(get_remote_all_events(), local_override_path=local_override_path))
