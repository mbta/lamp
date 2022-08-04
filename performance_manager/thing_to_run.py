#!/usr/bin/env python

import argparse
import logging
import os
import sys
import json

from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker

from lib import (
    process_all_static_schedules,
    get_local_engine,
    get_experimental_engine,
    SqlBase,
    StaticSubHeadway
)

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point to RDS Manipulating Performance Manager Scripts"""


def load_environment():
    """
    boostrap .env file for local development
    """
    bootstrap = False
    try:
        db_host = os.environ["DB_HOST"]
    except KeyError:
        bootstrap = True
    except Exception as exception:
        logging.error("error while trying to bootstrap")
        logging.exception(exception)
    else:
        return

    try:
        HERE = os.path.dirname(os.path.abspath(__file__))
        env_file = os.path.join(HERE, "..", ".env")
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r") as reader:
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

def parse_args(args: list[str]) -> dict:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--experimental",
        action='store_true',
        dest='experimental',
        help="if set, use a sqllite engine for quicker development"
    )

    return parser.parse_args(args)

def write_to_db_test():
    logging.info("Createing Engine to DataBase")
    engine = get_local_engine()

    metadata_obj = MetaData()
    metadata_obj.bind = engine

    user_table = Table(
        "user",
        metadata_obj,
        Column("user_id", Integer, primary_key=True),
        Column("user_name", String(16), nullable=False),
        Column("email_address", String(60)),
        Column("nickname", String(50), nullable=False),
    )

    for t in metadata_obj.sorted_tables:
        logging.info(t.name)

    for c in user_table.c:
        logging.info(c)

    metadata_obj.create_all()


def read_from_parquet_test():
    gtfs_subway_headways = process_all_static_schedules()

    # The orient='records' is the key of this, it allows to align with the
    # format mentioned in the doc to insert in bulks.
    return gtfs_subway_headways.to_dict(orient="records")


def read_from_json():
    HERE = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(HERE, "derp.json")
    logging.info("loading file %s", filepath)

    with open(filepath, encoding="utf8") as gtfs_subway_json:
        gtfs_subway_dict: dict = json.load(gtfs_subway_json)

    return gtfs_subway_dict


def write_from_dict(subway_headways_dict, experimental):
    try:
        if experimental:
            SqlEngine = get_experimental_engine()
        else:
            SqlEngine = get_local_engine()

        SqlSession = sessionmaker(bind=SqlEngine)
        SqlBase.metadata.create_all(SqlEngine)

        with SqlSession.begin() as session:
            result = session.execute(
                insert(StaticSubHeadway.__table__),
                subway_headways_dict)
            session.commit()
    except Exception as e:
        logging.error("error writing item")
        logging.info(item)
        logging.exception(e)


if __name__ == "__main__":
    load_environment()
    parsed_args = parse_args(sys.argv[1:])

    gtfs_subway_headways_dict = read_from_parquet_test()

    write_from_dict(
            gtfs_subway_headways_dict,
            experimental=parsed_args.experimental
    )
