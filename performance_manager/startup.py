#!/usr/bin/env python

import argparse
import json
import logging
import os
import sched
import sys
import time

from typing import List

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from lib import (
    SqlBase,
    MetadataLog,
    get_experimental_engine,
    get_local_engine,
    process_vehicle_positions,
    unprocessed_files,
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


def seed_metadata_table(sql_session: sessionmaker) -> None:
    """
    add s3 filepaths to metadata table
    TODO(team) make the filepath an input argument as well
    """
    try:
        seed_file = os.path.join(HERE, "tests", "july_17_filepaths.json")
        with open(seed_file, "r", encoding="utf8") as seed_json:
            load_paths = json.load(seed_json)

        with sql_session.begin() as session:  # type: ignore
            session.execute(sa.insert(MetadataLog.__table__), load_paths)
            session.commit()
    except Exception as e:
        logging.error("Error cleaning and seeding database")
        logging.exception(e)


def main(args: argparse.Namespace) -> None:
    """entrypoint into performance manager event loop"""
    # get the engine that manages sessions that read and write to the db
    if args.experimental:
        sql_engine = get_experimental_engine(echo=True)
    else:
        sql_engine = get_local_engine(echo=True)

    # create a session factory and ensure that all of our tables are in place
    sql_session = sessionmaker(bind=sql_engine)
    SqlBase.metadata.create_all(sql_engine)

    # if --seed, then drop the metadata table and load in predescribed paths.
    if args.seed:
        seed_metadata_table(sql_session)

    # schedule object that will control the "event loop"
    scheduler = sched.scheduler(time.time, time.sleep)

    # function to call each time on the event loop, rescheduling the loop at the
    # end of each iteration
    def iteration() -> None:
        """function to invoke on a scheduled routine"""
        logging.info("Entering Iteration")

        paths = unprocessed_files(sql_session)
        process_vehicle_positions(paths, sql_session)

        scheduler.enter(int(args.interval), 1, iteration)

    # schedule the inital looop and start the scheduler
    scheduler.enter(0, 1, iteration)
    scheduler.run()


if __name__ == "__main__":
    load_environment()
    parsed_args = parse_args(sys.argv[1:])

    main(parsed_args)
