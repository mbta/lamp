#!/usr/bin/env python

import argparse
import logging
import os
import sys

from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker

from lib import (
    SqlBase,
    StaticSubHeadway,
    get_experimental_engine,
    get_local_engine,
    process_static_schedule,
)

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point to RDS Manipulating Performance Manager Scripts"""


def load_environment() -> None:
    """
    boostrap .env file for local development
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        here = os.path.dirname(os.path.abspath(__file__))
        env_file = os.path.join(here, "..", ".env")
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


def parse_args(args: list[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--experimental",
        action="store_true",
        dest="experimental",
        help="if set, use a sqllite engine for quicker development",
    )

    return parser.parse_args(args)


def get_static_schedule() -> dict:
    """
    try to read a static schedule for the 1659568605 timestamp for testing
    things out.
    """
    gtfs_subway_headways = process_static_schedule("1659568605")

    # The orient='records' is the key of this, it allows to align with the
    # format mentioned in the doc to insert in bulks.
    return gtfs_subway_headways.to_dict(orient="records")


def write_from_dict(input_dictionary: dict, experimental: bool) -> None:
    """
    try to write a dict to a table. if experimental, use the testing engine,
    else use the local one.
    """
    try:
        if experimental:
            sql_engine = get_experimental_engine(echo=True)
        else:
            sql_engine = get_local_engine(echo=True)

        sql_session = sessionmaker(bind=sql_engine)
        SqlBase.metadata.create_all(sql_engine)

        with sql_session.begin() as session:  # type: ignore
            session.execute(
                insert(StaticSubHeadway.__table__), input_dictionary
            )
            session.commit()
    except Exception as e:
        logging.error("Error Writing Dataframe to Database")
        logging.exception(e)


if __name__ == "__main__":
    load_environment()
    parsed_args = parse_args(sys.argv[1:])

    gtfs_subway_headways_dict = get_static_schedule()

    write_from_dict(
        gtfs_subway_headways_dict, experimental=parsed_args.experimental
    )
