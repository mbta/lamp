import asyncio
from datetime import timedelta
from os import environ
from signal import SIGTERM, signal

import dataframely as dy

from lamp_py.aws.ecs import handle_ecs_sigterm
from lamp_py.flashback.events import StopEventsTable, structure_stop_events, unnest_vehicle_positions, update_records
from lamp_py.flashback.io import get_remote_events, get_vehicle_positions, write_stop_events
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger


async def flashback(
    remote_events: dy.DataFrame[StopEventsTable], max_record_age: timedelta = timedelta(hours=2)
) -> None:
    """Fetch, process, and store stop events."""
    existing_events = remote_events
    while True:
        process_logger = ProcessLogger("flashback")
        process_logger.log_start()
        new_records = await get_vehicle_positions()

        stop_events = update_records(existing_events, unnest_vehicle_positions(new_records), max_record_age)

        existing_events = stop_events

        await asyncio.to_thread(lambda: write_stop_events(structure_stop_events(stop_events)))

        process_logger.log_complete()

        await asyncio.sleep(3)  # wait before fetching new data


def pipeline() -> None:
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

    asyncio.run(flashback(get_remote_events()))
