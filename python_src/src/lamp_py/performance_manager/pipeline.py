#!/usr/bin/env python

import argparse
import logging
import os
import sched
import sys
import time
import signal
from typing import List

import sqlalchemy as sa

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.postgres.postgres_utils import DatabaseManager, DatabaseIndex
from lamp_py.postgres.rail_performance_manager_schema import LegacyMetadataLog
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.tableau.pipeline import start_parquet_updates

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


def legacy_metadata_exists(rpm_db_manager: DatabaseManager) -> bool:
    """
    are there any records in the legacy metadata table
    """
    metadata = rpm_db_manager.select_as_dataframe(
        sa.select(LegacyMetadataLog.path)
    )

    return not metadata.empty


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # get the engine that manages sessions that read and write to the db
    rpm_db_manager = DatabaseManager(
        db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER, verbose=args.verbose
    )
    md_db_manager = DatabaseManager(
        db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER, verbose=args.verbose
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
            # temporary check to see if legacy metadata exists. if it does,
            # raise an exception and try again on the next cycle.
            if legacy_metadata_exists(rpm_db_manager):
                raise EnvironmentError("Legacy Metadata Detected")

            process_static_tables(rpm_db_manager, md_db_manager)
            process_gtfs_rt_files(rpm_db_manager, md_db_manager)
            write_flat_files(rpm_db_manager)
            start_parquet_updates(rpm_db_manager)

            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)
        finally:
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
            "ALEMBIC_DB_NAME",
        ],
        optional_variables=["PUBLIC_ARCHIVE_BUCKET"],
        validate_db=True,
    )

    # run rail performance manager rds migrations
    alembic_upgrade_to_head(db_name=os.getenv("ALEMBIC_DB_NAME"))

    # run main method with parsed args
    main(parsed_args)


if __name__ == "__main__":
    start()
