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
from lamp_py.bus_performance_manager.write_events import regenerate_bus_metrics_recent, write_bus_metrics

from lamp_py.tableau.jobs.bus_performance import BUS_RECENT_NDAYS
from lamp_py.tableau.pipeline import start_bus_parquet_updates

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to Bus Performance Manager"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--interval",
        default=300,
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
            write_bus_metrics()
            regenerate_bus_metrics_recent(num_days=BUS_RECENT_NDAYS)
            start_bus_parquet_updates()
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
    )

    # run main method
    main(parsed_args)


if __name__ == "__main__":
    start()
