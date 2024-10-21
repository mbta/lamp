#!/usr/bin/env python

import os
import argparse
import json
import logging
import sys
from typing import List

import sqlalchemy as sa

from lamp_py.runtime_utils.alembic_migration import (
    alembic_upgrade_to_head,
    alembic_downgrade_to_base,
)

from .rail_performance_manager_schema import (
    VehicleEvents,
    VehicleTrips,
)
from .metadata_schema import MetadataLog
from .postgres_utils import DatabaseManager, DatabaseIndex, seed_metadata


logging.getLogger().setLevel("INFO")

DESCRIPTION = """Interact with Performance Manager RDS for Testing"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)

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


def reset_rpm(parsed_args: argparse.Namespace) -> None:
    """
    reset values in the rail performance manager database if requested.
    """
    try:
        logging.info("Resetting Rail Performance Manager DB")
        rpm_db_manager = DatabaseManager(
            db_index=DatabaseIndex.RAIL_PERFORMANCE_MANAGER,
            verbose=parsed_args.verbose,
        )

        if parsed_args.clear_static:
            rpm_db_name = os.getenv("ALEMBIC_RPM_DB_NAME", "")
            alembic_downgrade_to_base(rpm_db_name)
            alembic_upgrade_to_head(rpm_db_name)
        elif parsed_args.clear_rt:
            rpm_db_manager.truncate_table(VehicleTrips, restart_identity=True)
            rpm_db_manager.truncate_table(VehicleEvents, restart_identity=True)
    except Exception as exception:
        logging.exception("Unable to Reset Rail Performance Manager DB\n%s", exception)


def run() -> None:
    """Run The RDS Interaction Script"""
    parsed_args = parse_args(sys.argv[1:])

    reset_rpm(parsed_args)

    md_db_manager = DatabaseManager(
        db_index=DatabaseIndex.METADATA,
        verbose=parsed_args.verbose,
    )

    if parsed_args.clear_static:
        md_db_name = os.getenv("ALEMBIC_MD_DB_NAME", "")
        alembic_downgrade_to_base(md_db_name)
        alembic_upgrade_to_head(md_db_name)
    elif parsed_args.clear_rt:
        md_db_manager.execute(
            sa.update(MetadataLog.__table__)
            .values(
                rail_pm_processed=False,
                rail_pm_process_fail=False,
            )
            .where(MetadataLog.path.like("%RT_%"))
        )

    if parsed_args.seed_file:
        try:
            logging.info("Seeding Metadata")

            with open(parsed_args.seed_file, "r", encoding="utf8") as seed_json:
                paths = json.load(seed_json)
            seed_metadata(md_db_manager, paths)

            logging.info("Seeding Metadata Completed")
        except Exception as exception:
            logging.exception("Seeding Metadata Failed %s", exception)


if __name__ == "__main__":
    run()
