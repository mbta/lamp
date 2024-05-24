#!/usr/bin/env python

import logging
import os
import signal

from lamp_py.aws.ecs import handle_ecs_sigterm
from lamp_py.runtime_utils.env_validation import validate_environment

from lamp_py.ingestion_tm.ingest import ingest_tables

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For TM Ingestion Scripts"""


def start() -> None:
    """configure and start the transitmaster ingestion process"""
    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    os.environ["SERVICE_NAME"] = "ingestion_tm"

    validate_environment(
        required_variables=[
            "SPRINGBOARD_BUCKET",
            "TM_DB_HOST",
            "TM_DB_NAME",
            "TM_DB_USER",
        ],
        private_variables=[
            "TM_DB_PASSWORD",
        ],
    )

    # run the main method
    ingest_tables()


if __name__ == "__main__":
    start()
