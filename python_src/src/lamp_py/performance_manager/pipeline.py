#!/usr/bin/env python

import argparse
import logging
import os
import gc
import sched
import sys
import time
import signal
from typing import List

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.postgres.postgres_utils import DatabaseManager, DatabaseIndex
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.tableau import start_parquet_updates
from lamp_py.tableau import clean_parquet_paths

from .flat_file import write_flat_files
from .l0_gtfs_rt_events import process_gtfs_rt_files
from .l0_gtfs_static_load import process_static_tables

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to Rail Performance Manager"""


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


def run_on_app_start() -> None:
    """Anything that should be run once at application start"""
    on_app_start_log = ProcessLogger("run_on_app_start")
    on_app_start_log.log_start()

    clean_parquet_paths()

    on_app_start_log.log_complete()


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # get the engine that manages sessions that read and write to the db
    rpm_db_manager = DatabaseManager(
        db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER, verbose=args.verbose
    )
    md_db_manager = DatabaseManager(
        db_index=DatabaseIndex.METADATA, verbose=args.verbose
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
            process_static_tables(rpm_db_manager, md_db_manager)
            process_gtfs_rt_files(rpm_db_manager, md_db_manager)
            write_flat_files(rpm_db_manager)
            # start_parquet_updates(rpm_db_manager) # FIXME: renable when live

            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)
        finally:
            gc.collect()
            scheduler.enter(int(args.interval), 1, iteration)

    # schedule the initial loop and start the scheduler
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
            "ALEMBIC_RPM_DB_NAME",
        ],
        optional_variables=["PUBLIC_ARCHIVE_BUCKET"],
        db_prefixes=["RPM", "MD"],
    )

    # run rail performance manager rds migrations
    alembic_upgrade_to_head(db_name=os.getenv("ALEMBIC_RPM_DB_NAME"))

    # run one time actions at every application deployment
    run_on_app_start()

    # run main method with parsed args
    main(parsed_args)


if __name__ == "__main__":
    start()
