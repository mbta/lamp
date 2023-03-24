import os
import logging

from alembic.config import Config
from alembic import command

from lamp_py.postgres.postgres_utils import DatabaseManager


def run_alembic_migration(db_name: str) -> None:
    """
    run alembic migration command at application startup
    """
    here = os.path.dirname(os.path.abspath(__file__))
    alembic_cfg_file = os.path.join(here, "..", "..", "..", "alembic.ini")
    alembic_cfg_file = os.path.abspath(alembic_cfg_file)
    logging.info("running alembic with config file %s", alembic_cfg_file)

    if db_name == "performance_manager":
        # if database is clean, call to DatabaseManager will trigger
        # SqlBase.metadata.create_all(self.engine)
        # creating all database tables
        db_manager = DatabaseManager()
    else:
        raise NotImplementedError(f"Migration for {db_name} not implemented.")

    # load alembic configuation for db_name
    alembic_cfg = Config(alembic_cfg_file, ini_section=db_name)

    # check if alembic_version table exists in rds
    try:
        db_manager.select_as_list("SELECT 1 FROM alembic_version")
    except Exception as _:
        # if no version table, create version table and stamp with current head
        # this assumes the head matches current rds state
        command.stamp(alembic_cfg, revision="head")

    # normal migration command, for if version table exists
    command.upgrade(alembic_cfg, revision="head")
