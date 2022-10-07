#!/usr/bin/env python

import logging
import os
import time

import schedule

from lib import (
    batch_files,
    file_list_from_s3,
    DEFAULT_S3_PREFIX,
    move_s3_objects,
    get_converter,
    write_parquet_file,
    ProcessLogger,
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


@schedule.repeat(schedule.every().hour.at(":05"))
def batch_and_ingest() -> None:
    """
    get all of the filepaths currently in the incoming bucket, sort them into
    batches of similar gtfs files, convert each batch into tables, write the
    tables to parquet files in the springboard bucket, add the parquet
    filepaths to the metadata table as unprocessed, and move gtfs files to the
    archive bucket (or error bucket in the event of an error)
    """
    process_logger = ProcessLogger("batch_and_ingest")
    process_logger.log_start()

    file_list = file_list_from_s3(
        bucket_name=os.environ["INCOMING_BUCKET"],
        file_prefix=DEFAULT_S3_PREFIX,
    )

    # TODO(zap) - do we want to keep threshold around? seems like we can remove
    # it now.
    for batch in batch_files(file_list, 1_000_000_000):
        files = batch.filenames
        archive_files = []
        error_files = []

        try:
            config_type = batch.config_type
            converter = get_converter(config_type)

            for s3_prefix, table in converter.convert(files):
                write_parquet_file(
                    table=table,
                    filetype=s3_prefix,
                    s3_path=os.path.join(
                        os.environ["SPRINGBOARD_BUCKET"],
                        DEFAULT_S3_PREFIX,
                        s3_prefix,
                    ),
                    partition_cols=converter.partition_cols,
                )

            archive_files = converter.archive_files
            error_files = converter.error_files

        except Exception as exception:
            logging.exception(
                "failed=convert_files, error_type=%s, config_type=%s, filecount=%d",
                type(exception).__name__,
                config_type,
                len(files),
            )

            # if unable to determine config from filename, or not implemented
            # yet, all files are marked as failed ingestion
            archive_files = []
            error_files = files

        finally:
            if len(error_files) > 0:
                move_s3_objects(
                    error_files,
                    os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
                )

            if len(archive_files) > 0:
                move_s3_objects(
                    archive_files,
                    os.path.join(
                        os.environ["ARCHIVE_BUCKET"], DEFAULT_S3_PREFIX
                    ),
                )


def main() -> None:
    """every second run jobs that are currently pending"""
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logging.info("Starting Ingestion Container")
    load_environment()
    validate_environment()
    main()
