#!/usr/bin/env python

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, List

import numpy
import sqlalchemy as sa
import pyarrow.parquet as pq

from lamp_py.runtime_utils.import_env import load_environment
from lamp_py.runtime_utils.alembic_migration import (
    alembic_upgrade_to_head,
    alembic_downgrade_to_base,
)

from .postgres_schema import SqlBase
from .postgres_utils import DatabaseManager


logging.getLogger().setLevel("INFO")

DESCRIPTION = """Create and Load Snapshots from a database"""


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        "--env-file",
        dest="env_file",
        help="environment file with rds connection parameters",
        required=False,
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        help="verbose logging from db manager",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("save")
    subparsers.add_parser("load")
    subparsers.add_parser("reset")

    return parser.parse_args(args)


def get_state_dir() -> Path:
    """
    get the directory holding all of the snapshot state parquet files
    """
    here = Path(__file__).parent
    return Path.joinpath(
        here,
        "..",
        "..",
        "..",
        "tests",
        "test_files",
        "state",
    ).absolute()


def get_table_filepath(table_name: str) -> Path:
    """
    get a filepath to the state directory for a given table.
    """
    return Path.joinpath(get_state_dir(), f"{table_name}.parquet")


def save_state(db_manager: DatabaseManager) -> None:
    """
    save the state of the database into local parquet files
    """
    state_dir = get_state_dir()
    if not state_dir.exists():
        state_dir.mkdir()

    for table_name, table in SqlBase.metadata.tables.items():
        if "temp" in table_name:
            continue

        dataframe = db_manager.select_as_dataframe(sa.select(table.columns))

        # no need to save pk id or updated on columns. we can't load them in
        # anyway.
        if "pk_id" in dataframe.columns:
            dataframe = dataframe.drop(columns=["pk_id"])
        if "updated_on" in dataframe.columns:
            dataframe = dataframe.drop(columns=["updated_on"])

        save_path = get_table_filepath(table_name)
        dataframe.to_parquet(save_path)


def reset_database() -> None:
    """
    clear out all tables in the database and restore them using the current
    schema version
    """
    alembic_downgrade_to_base("performance_manager")
    alembic_upgrade_to_head("performance_manager")


def load_state(db_manager: DatabaseManager) -> None:
    """
    load the state of a database from local parquet files
    """

    def restore_table(table_name: str, table: Any) -> None:
        """
        load a parquet snapshot of a table and insert it into the database
        """
        logging.info("Restoring %s table", table_name)
        load_path = get_table_filepath(table_name)
        if not os.path.exists(load_path):
            logging.error(
                "Couldn't find %s parquet file. Skipping.", table_name
            )
            return

        dataframe = pq.ParquetDataset(load_path).read_pandas().to_pandas()

        dataframe = dataframe.fillna(numpy.nan).replace([numpy.nan], [None])

        db_manager.insert_dataframe(dataframe, table)

    state_dir = get_state_dir()
    if not state_dir.exists():
        raise FileNotFoundError(f"State Dir {state_dir} does not exist")

    reset_database()

    # some of the tables have foreign keys to elements in other tables. store
    # them in this map and load them in once all the tables without
    # dependencies are loaded.
    dependent_tables = {}

    for table_name, table in SqlBase.metadata.tables.items():
        # skip temp tables. we don't need to bring them back
        if "temp" in table_name:
            continue
        # for now the easiest way to handle these dependencies is to load the
        # gtfs static schedule first. there is probably a programatic way to do
        # it though.
        if "static" not in table_name:
            dependent_tables[table_name] = table
            continue
        restore_table(table_name, table)

    for table_name, table in dependent_tables.items():
        restore_table(table_name, table)


def run() -> None:
    """Run The RDS Interaction Script"""
    parsed_args = parse_args(sys.argv[1:])

    load_environment(parsed_args.env_file)

    db_manager = DatabaseManager(parsed_args.verbose)

    if parsed_args.command == "save":
        save_state(db_manager)
    elif parsed_args.command == "load":
        load_state(db_manager)
    elif parsed_args.command == "reset":
        reset_database()


if __name__ == "__main__":
    run()
