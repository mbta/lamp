import logging
import os
from typing import Iterator

import pytest

from lib.postgres_utils import (
    DatabaseManager,
    get_unprocessed_files,
)

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(autouse=True)
def set_environment_variables() -> Iterator[None]:
    """
    boostrap .env file for local testing
    """
    if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
        logging.warning("allready bootstrapped")
    else:
        env_file = os.path.join(HERE, "..", "..", ".env")
        logging.warning("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    yield


def test_environ() -> None:
    """simple test stub"""
    # ensure that the environment has been stook up correctly.
    assert int(os.environ.get("BOOTSTRAPPED", 0)) == 1

    db_manager = DatabaseManager()

    paths_to_load = get_unprocessed_files(
        "RT_VEHICLE_POSITIONS", db_manager, file_limit=6
    )

    assert len(paths_to_load) == 0
