#!/usr/bin/env python

import argparse
import logging
import os
import sched
import signal
import sys
import time
from typing import List

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to Bus Performance Manager"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--interval",
        default=60,
        dest="interval",
        help="interval to run event loop on",
    )

    return parser.parse_args(args)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # schedule object that will control the "event loop"
    scheduler = sched.scheduler(time.time, time.sleep)

    # function to call each time on the event loop, rescheduling the loop at the
    # end of each iteration
    def iteration() -> None:
        """function to invoke on a scheduled routine"""
        check_for_sigterm()
        process_logger = ProcessLogger("event_loop")
        process_logger.log_start()

        try:
            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)
        finally:
            scheduler.enter(int(args.interval), 1, iteration)

    # schedule the initial loop and start the scheduler
    scheduler.enter(0, 1, iteration)
    scheduler.run()
    main_process_logger.log_complete()


def start() -> None:
    """configure and start the bus performance manager process"""
    # parse arguments from the command line
    parsed_args = parse_args(sys.argv[1:])

    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    os.environ["SERVICE_NAME"] = "bus_performance_manager"
    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "PUBLIC_ARCHIVE_BUCKET",
            "SERVICE_NAME",
        ],
        validate_db=True,
    )

    # run main method
    main(parsed_args)


if __name__ == "__main__":
    start()
