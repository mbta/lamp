#!/usr/bin/env python

import logging
import os
import signal
from multiprocessing import Queue
from typing import List

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
        process_logger.add_metadata(**{key:value})

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


def ingest(metadata_queue: Queue) -> List[str]:
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

    processed_files = ingest_files(files, metadata_queue)

    process_logger.log_complete()

    return processed_files


def wait_for_deletion(filenames: List[str]) -> None:
    """
    wait for all of the files in the filenames list to be removed from the
    incoming bucket. log how many files are still
    """
    process_logger = ProcessLogger(
        "wait_for_deletion", file_count=len(filenames)
    )
    process_logger.log_start()

    files_to_delete = set(filenames)
    attempts = 0
    while len(files_to_delete) > 0 and attempts < 5:
        existing_files = set(
            file_list_from_s3(
                bucket_name=os.environ["INCOMING_BUCKET"],
                file_prefix=DEFAULT_S3_PREFIX,
            )
        )

        files_to_delete &= existing_files
        if len(files_to_delete) == 0:
            break

        # increment attempts and wait at most 30 seconds to check again
        attempts += 1
        time.sleep(min(30, int(len(files_to_delete) / 100)))

    process_logger.add_metadata(
        attempts=attempts, files_not_deleted=list(files_to_delete)
    )
    process_logger.log_complete()


def main() -> None:
    """
    run the ingestion pipeline

    * setup metadata queue metadata writer proccess
    * on a loop
        * check to see if the pipeline should be terminated
        * ingest files from incoming s3 bucket
        * ensure processed files have been moved
    """
    # start rds writer process
    # this will create only one rds engine while app is running
    metadata_queue, rds_process = start_rds_writer_process()

    # run the event loop every five minutes
    while True:
        check_for_sigterm(metadata_queue, rds_process)
        processed_files = ingest(metadata_queue=metadata_queue)
        wait_for_deletion(processed_files)
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
