#!/usr/bin/env python

import argparse
import json
import logging
import sys
from typing import List

import sqlalchemy as sa

from lamp_py.runtime_utils.import_env import load_environment
from lamp_py.runtime_utils.alembic_migration import (
    alembic_upgrade_to_head,
    alembic_downgrade_to_base,
)

from .postgres_schema import (
    MetadataLog,
    VehicleEventMetrics,
    VehicleEvents,
    VehicleTrips,
)
from .postgres_utils import DatabaseManager


logging.getLogger().setLevel("INFO")

DESCRIPTION = """Interact with Performance Manager RDS for Testing"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        "--env-file",
        dest="env_file",
        help="environment file containing rds connection parameters",
        required=False,
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        help="if set, use debug logging",
    )

    parser.add_argument(
        "--seed-file",
        dest="seed_file",
        help="if set, read a json file to seed the metadata table with",
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
        help="if set, clear gtfs static database tables and rt tables",
    )

    return parser.parse_args(args)


def seed_metadata(db_manager: DatabaseManager, seed_file: str) -> None:
    """
    seed metadata table for dev environment
    """
    logging.info("Seeding Metadata From File %s", seed_file)

    try:
        with open(seed_file, "r", encoding="utf8") as seed_json:
            paths = json.load(seed_json)

        db_manager.add_metadata_paths(paths)

        logging.info("Seeding Metadata Complete")

    except Exception as exception:
        logging.exception("Seeding Metadata Failed %s", exception)


def run() -> None:
    """Run The RDS Interaction Script"""
    parsed_args = parse_args(sys.argv[1:])

    load_environment(parsed_args.env_file)

    db_manager = DatabaseManager(parsed_args.verbose)

    if parsed_args.clear_static:
        alembic_downgrade_to_base("performance_manager")
        alembic_upgrade_to_head("performance_manager")
    elif parsed_args.clear_rt:
        db_manager.truncate_table(VehicleTrips)
        db_manager.truncate_table(VehicleEventMetrics)
        db_manager.truncate_table(VehicleEvents)

        db_manager.execute(
            sa.update(MetadataLog.__table__)
            .values(
                processed=False,
                process_fail=False,
            )
            .where(MetadataLog.path.like("%RT_%"))
        )

    if parsed_args.seed_file:
        seed_metadata(db_manager, parsed_args.seed_file)


if __name__ == "__main__":
    run()
