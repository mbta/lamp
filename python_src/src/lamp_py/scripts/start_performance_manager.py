#!/usr/bin/env python

import argparse
import logging
import os
import sched
import sys
import time
import signal
from typing import List

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.import_env import load_environment
from lamp_py.logging_utils import ProcessLogger
from lamp_py.performance_manager import (
    process_static_tables,
    process_gtfs_rt_files,
    process_trips_and_metrics,
)
from lamp_py.postgres.postgres_utils import DatabaseManager

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to Performance Manager"""


def validate_environment() -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    # these variables required for normal opperation, ensure they are present
    required_variables = [
        "SPRINGBOARD_BUCKET",
        "DB_HOST",
        "DB_NAME",
        "DB_PORT",
        "DB_USER",
    ]

    missing_required = [
        key for key in required_variables if os.environ.get(key, None) is None
    ]

    # if db password is missing, db region is required to generate a token to
    # use as the password to the cloud database
    if os.environ.get("DB_PASSWORD", None) is None:
        if os.environ.get("DB_REGION", None) is None:
            missing_required.append("DB_REGION")

    if missing_required:
        raise EnvironmentError(
            f"Missing required environment variables {missing_required}"
        )


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--experimental",
        action="store_true",
        dest="experimental",
        help="if set, use a sqllite engine for quicker development",
    )

    parser.add_argument(
        "--interval",
        default=60,
        dest="interval",
        help="interval to run event loop on",
    )

    parser.add_argument(
        "--seed",
        action="store_true",
        dest="seed",
        help="if set, seed metadataLog table with paths",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        help="if set, verbose sql logging",
    )

    parser.add_argument(
        "--clear-rt",
        action="store_true",
        dest="clear_rt",
        help="if set, clear gtfs-rt database tables",
    )

    parser.add_argument(
        "--clear-static",
        action="store_true",
        dest="clear_static",
        help="if set, clear gtfs static database tables",
    )

    return parser.parse_args(args)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # get the engine that manages sessions that read and write to the db
    db_manager = DatabaseManager(
        verbose=args.verbose,
        seed=args.seed,
        clear_rt=args.clear_rt,
        clear_static=args.clear_static,
    )

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
            process_static_tables(db_manager)
            process_gtfs_rt_files(db_manager)
            process_trips_and_metrics(db_manager)

            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)
        finally:
            scheduler.enter(int(args.interval), 1, iteration)

    # schedule the inital looop and start the scheduler
    scheduler.enter(0, 1, iteration)
    scheduler.run()


def start() -> None:
    """configure and start the performance manager process"""
    # parse arguments from the command line
    parsed_args = parse_args(sys.argv[1:])

    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    load_environment()
    os.environ["SERVICE_NAME"] = "performance_manager"
    validate_environment()

    # run main method with parsed args
    main(parsed_args)


if __name__ == "__main__":
    start()