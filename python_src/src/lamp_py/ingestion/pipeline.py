#!/usr/bin/env python

import logging
import os
import signal
from multiprocessing import Queue
import multiprocessing

import time

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .ingest import ingest_files
from .utils import DEFAULT_S3_PREFIX

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For GTFS Ingestion Scripts"""
HERE = os.path.dirname(os.path.abspath(__file__))


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
        time.sleep(5)


def start() -> None:
    """configure and start the ingestion process"""
    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    os.environ["SERVICE_NAME"] = "ingestion"

    validate_environment(
        required_variables=[
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
            "INCOMING_BUCKET",
            "SPRINGBOARD_BUCKET",
        ],
        validate_db=True,
    )

    # try forcing multiprocessing to use spawn
    multiprocessing.set_start_method("spawn")

    # run the main method
    main()


if __name__ == "__main__":
    start()
