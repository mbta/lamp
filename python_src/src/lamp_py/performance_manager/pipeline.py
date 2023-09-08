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
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .flat_file import write_flat_files
from .l0_gtfs_rt_events import process_gtfs_rt_files
from .l0_gtfs_static_load import process_static_tables

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to Performance Manager"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--interval",
        default=60,
        dest="interval",
        help="interval to run event loop on",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        help="if set, verbose sql logging",
    )

    return parser.parse_args(args)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # get the engine that manages sessions that read and write to the db
    db_manager = DatabaseManager(verbose=args.verbose)

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
            write_flat_files(db_manager)

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
    os.environ["SERVICE_NAME"] = "performance_manager"
    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "SERVICE_NAME",
            "ALEMBIC_DB_NAME",
        ],
        optional_variables=["PUBLIC_ARCHIVE_BUCKET"],
        validate_db=True,
    )

    # run rds migrations
    alembic_upgrade_to_head(db_name=os.getenv("ALEMBIC_DB_NAME"))

    # run main method with parsed args
    main(parsed_args)


if __name__ == "__main__":
    start()
