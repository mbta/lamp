#!/usr/bin/env python

import argparse
import logging
import os
import sched
import sys
import time

from typing import List

from lib import (
    DatabaseManager,
    ProcessLogger,
    process_vehicle_positions,
    process_trip_updates,
    process_static_tables,
    process_full_trip_events,
    process_dwell_travel_times,
    process_headways,
)

logging.getLogger().setLevel("INFO")

DESCRIPTION = """Entry Point to RDS Manipulating Performance Manager Scripts"""
HERE = os.path.dirname(os.path.abspath(__file__))


def load_environment() -> None:
    """
    boostrap .env file for local development
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        env_file = os.path.join(HERE, "..", ".env")
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    except FileNotFoundError as fnfe:
        logging.warning("unable to find env file %s", fnfe)
    except Exception as exception:
        logging.exception("error while trying to bootstrap")
        raise exception


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
        raise Exception(
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

    return parser.parse_args(args)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    main_process_logger = ProcessLogger("main", **vars(args))
    main_process_logger.log_start()

    # get the engine that manages sessions that read and write to the db
    db_manager = DatabaseManager(verbose=args.verbose, seed=args.seed)

    # schedule object that will control the "event loop"
    scheduler = sched.scheduler(time.time, time.sleep)

    # function to call each time on the event loop, rescheduling the loop at the
    # end of each iteration
    def iteration() -> None:
        """function to invoke on a scheduled routine"""
        process_logger = ProcessLogger("event_loop")
        process_logger.log_start()

        try:
            process_static_tables(db_manager)
            process_vehicle_positions(db_manager)
            process_trip_updates(db_manager)
            process_full_trip_events(db_manager)
            process_dwell_travel_times(db_manager)
            process_headways(db_manager)

            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)
        finally:
            scheduler.enter(int(args.interval), 1, iteration)

    # schedule the inital looop and start the scheduler
    scheduler.enter(0, 1, iteration)
    scheduler.run()


if __name__ == "__main__":
    load_environment()
    validate_environment()
    parsed_args = parse_args(sys.argv[1:])

    main(parsed_args)
