#!/usr/bin/env python

import logging
import os
import signal
from multiprocessing import Queue

import time
import schedule

from lib import (
    ingest_files,
    file_list_from_s3,
    DEFAULT_S3_PREFIX,
    ProcessLogger,
    start_rds_writer_process,
    handle_ecs_sigterm,
    check_for_sigterm,
)

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For GTFS Ingestion Scripts"""
HERE = os.path.dirname(os.path.abspath(__file__))


def load_environment() -> None:
    """
    boostrap .env file for local development
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        env_file = os.path.join(HERE, "..", ".env")
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    except FileNotFoundError as fnfe:
        logging.warning("unable to find env file %s", fnfe)
    except Exception as exception:
        logging.exception("error while trying to bootstrap")
        raise exception


def validate_environment() -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    # these variables required for normal opperation, ensure they are present
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

    missing_required = [
        key for key in required_variables if os.environ.get(key, None) is None
    ]

    # if db password is missing, db region is required to generate a token to
    # use as the password to the cloud database
    if os.environ.get("DB_PASSWORD", None) is None:
        if os.environ.get("DB_REGION", None) is None:
            missing_required.append("DB_REGION")

    if missing_required:
        raise Exception(
            f"Missing required environment variables {missing_required}"
        )


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
    """every second run jobs that are currently pending"""
    # start rds writer process
    # this will create only one rds engine while app is running
    metadata_queue, _rds_process = start_rds_writer_process()

    schedule.every(5).minutes.do(ingest, metadata_queue=metadata_queue)

    while True:
        check_for_sigterm(metadata_queue)
        schedule.run_pending()
        time.sleep(1)

    # send shutdown signal to rds process and wait for finish
    # not sure if we'll ever get here the way we're using @schedule
    # metadata_queue.put(None)
    # _rds_process.join()


if __name__ == "__main__":
    logging.info("Starting Ingestion Container")
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)
    load_environment()
    validate_environment()
    main()
