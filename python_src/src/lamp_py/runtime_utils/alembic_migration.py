import os
import logging

from alembic.config import Config
from alembic import command


def run_alembic_migration(db_name: str) -> None:
    """
    run alembic migration command at application startup
    """
    here = os.path.dirname(os.path.abspath(__file__))
    alembic_cfg_file = os.path.join(here, "..", "..", "..", "alembic.ini")
    alembic_cfg_file = os.path.abspath(alembic_cfg_file)
    logging.info("running alembic with config file %s", alembic_cfg_file)

    if db_name == "performance_manager":
        pass
    else:
        raise NotImplementedError(f"Migration for {db_name} not implemented.")

    # load alembic configuation for db_name
    alembic_cfg = Config(alembic_cfg_file, ini_section=db_name)

    # normal migration command, for if version table exists
    command.upgrade(alembic_cfg, revision="head")
