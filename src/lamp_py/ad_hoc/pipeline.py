#!/usr/bin/env python

import logging
import os

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.ad_hoc.runner_001 import runner

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For Ad-Hoc Runner"""


def start() -> None:
    """configure and start the ad-hoc runner"""
    # configure the environment
    os.environ["SERVICE_NAME"] = "ad_hoc"

    validate_environment(
        required_variables=[
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
            "INCOMING_BUCKET",
            "PUBLIC_ARCHIVE_BUCKET",
            "SPRINGBOARD_BUCKET",
        ],
        db_prefixes=["MD", "RPM"],
    )

    check_for_parallel_tasks()

    # run the main method
    runner()


if __name__ == "__main__":
    start()
