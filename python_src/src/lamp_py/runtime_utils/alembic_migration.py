import os
import logging

from alembic.config import Config
from alembic import command


def get_alembic_config(db_name: str) -> Config:
    """
    get alembic configuration for specified db_name

    will raise NotImplementedError if db_name is not supported
    """
    here = os.path.dirname(os.path.abspath(__file__))
    alembic_cfg_file = os.path.join(here, "..", "..", "..", "alembic.ini")
    alembic_cfg_file = os.path.abspath(alembic_cfg_file)
    logging.info(
        "getting alembic config for %s from %s", db_name, alembic_cfg_file
    )

    if db_name == "performance_manager":
        pass
    else:
        raise NotImplementedError(f"Migration for {db_name} not implemented.")

    return Config(alembic_cfg_file, ini_section=db_name)


def alembic_upgrade_to_head(db_name: str) -> None:
    """
    upgrade db_name to head revision
    """
    # load alembic configuation for db_name
    alembic_cfg = get_alembic_config(db_name)

    command.upgrade(alembic_cfg, revision="head")


def alembic_downgrade_to_base(db_name: str) -> None:
    """
    downgrade db_name to base revision
    """
    # load alembic configuation for db_name
    alembic_cfg = get_alembic_config(db_name)

    command.downgrade(alembic_cfg, revision="base")
