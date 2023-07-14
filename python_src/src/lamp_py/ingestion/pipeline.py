#!/usr/bin/env python

import logging
import os
import signal
from multiprocessing import Queue

import time

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .ingest import ingest_files
from .utils import DEFAULT_S3_PREFIX

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For GTFS Ingestion Scripts"""
HERE = os.path.dirname(os.path.abspath(__file__))


def validate_environment() -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    process_logger = ProcessLogger("validate_env")
    process_logger.log_start()

    # these variables required for normal operation, ensure they are present
    required_variables = [
        "ARCHIVE_BUCKET",
        "ERROR_BUCKET",
        "INCOMING_BUCKET",
        "SPRINGBOARD_BUCKET",
        "DB_HOST",
        "DB_NAME",
        "DB_PORT",
        "DB_USER",
        "INCOMING_BUCKET",
    ]

    missing_required = []
    for key in required_variables:
        value = os.environ.get(key, None)
        if value is None:
            missing_required.append(key)
        process_logger.add_metadata(**{key: value})

    # if db password is missing, db region is required to generate a token to
    # use as the password to the cloud database
    if os.environ.get("DB_PASSWORD", None) is None:
        value = os.environ.get("DB_REGION", None)
        if value is None:
            missing_required.append("DB_REGION")
        process_logger.add_metadata(DB_REGION=value)

    if missing_required:
        exception = EnvironmentError(
            f"Missing required environment variables {missing_required}"
        )
        process_logger.log_failure(exception)
        raise exception

    process_logger.log_complete()


def ingest(metadata_queue: Queue) -> None:
    """
    get all of the filepaths currently in the incoming bucket, sort them into
    batches of similar gtfs files, convert each batch into tables, write the
    tables to parquet files in the springboard bucket, add the parquet
    filepaths to the metadata table as unprocessed, and move gtfs files to the
    archive bucket (or error bucket in the event of an error)
    """
    process_logger = ProcessLogger("ingest_all")
    process_logger.log_start()

    files = file_list_from_s3(
        bucket_name=os.environ["INCOMING_BUCKET"],
        file_prefix=DEFAULT_S3_PREFIX,
    )

    ingest_files(files, metadata_queue)

    process_logger.log_complete()


def main() -> None:
    """
    run the ingestion pipeline

    * setup metadata queue metadata writer proccess
    * on a loop
        * check to see if the pipeline should be terminated
        * ingest files from incoming s3 bucket
    """
    # start rds writer process
    # this will create only one rds engine while app is running
    metadata_queue, rds_process = start_rds_writer_process()

    # run the event loop every five minutes
    while True:
        check_for_sigterm(metadata_queue, rds_process)
        ingest(metadata_queue=metadata_queue)
        check_for_sigterm(metadata_queue, rds_process)
        time.sleep(60 * 5)


def start() -> None:
    """configure and start the ingestion process"""
    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    os.environ["SERVICE_NAME"] = "ingestion"
    validate_environment()

    # run the main method
    main()


if __name__ == "__main__":
    start()
