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
    process_vehicle_positions,
    process_trip_updates,
    process_static_tables,
    process_full_trip_events,
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

    except Exception as exception:
        logging.error("error while trying to bootstrap")
        logging.exception(exception)
        raise exception


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

    return parser.parse_args(args)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    # get the engine that manages sessions that read and write to the db
    db_manager = DatabaseManager(experimental=args.experimental, verbose=True)

    # if --seed, then drop the metadata table and load in predescribed paths.
    if args.seed:
        db_manager.seed_metadata()

    # schedule object that will control the "event loop"
    scheduler = sched.scheduler(time.time, time.sleep)

    # function to call each time on the event loop, rescheduling the loop at the
    # end of each iteration
    def iteration() -> None:
        """function to invoke on a scheduled routine"""
        logging.info("Entering Iteration")

        process_static_tables(db_manager)
        process_vehicle_positions(db_manager)
        process_trip_updates(db_manager)
        process_full_trip_events(db_manager)

        scheduler.enter(int(args.interval), 1, iteration)

    # schedule the inital looop and start the scheduler
    scheduler.enter(0, 1, iteration)
    scheduler.run()


if __name__ == "__main__":
    load_environment()
    parsed_args = parse_args(sys.argv[1:])

    main(parsed_args)
