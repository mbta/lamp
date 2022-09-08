#!/usr/bin/env python

import argparse
import json
import logging
import os
import sys

from typing import Dict, Union

from py_gtfs_rt_ingestion import ArgumentException
from py_gtfs_rt_ingestion import ConfigType
from py_gtfs_rt_ingestion import DEFAULT_S3_PREFIX
from py_gtfs_rt_ingestion import LambdaContext
from py_gtfs_rt_ingestion import LambdaDict
from py_gtfs_rt_ingestion import ProcessLogger
from py_gtfs_rt_ingestion import get_converter
from py_gtfs_rt_ingestion import load_environment
from py_gtfs_rt_ingestion import move_s3_objects
from py_gtfs_rt_ingestion import unpack_filenames
from py_gtfs_rt_ingestion import write_parquet_file

logging.getLogger().setLevel("INFO")

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."


def parse_args(args: list[str]) -> Union[LambdaDict, list[LambdaDict]]:
    """
    parse input args from the command line. using them, generate an event
    lambdadict object and set environment variables.
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--input",
        dest="input_file",
        type=str,
        help="provide filename to ingest",
    )

    parser.add_argument(
        "--event-json",
        dest="event_json",
        type=str,
        help="lambda event json file",
    )

    parsed_args = parser.parse_args(args)

    if parsed_args.event_json is not None:
        with open(parsed_args.event_json, encoding="utf8") as event_json_file:
            events: dict = json.load(event_json_file)

        return events

    return {"files": [(parsed_args.input_file)]}


def main(event: Dict, process_logger: ProcessLogger) -> None:
    """
    * Convert a list of files from s3 to a parquet table
    * Write the table out to s3
    * Archive processed json files to archive s3 bucket
    * Move files that generated error to error s3 bucket
    """
    try:
        export_bucket = os.path.join(
            os.environ["EXPORT_BUCKET"], DEFAULT_S3_PREFIX
        )
        archive_bucket = os.path.join(
            os.environ["ARCHIVE_BUCKET"], DEFAULT_S3_PREFIX
        )
        error_bucket = os.path.join(
            os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX
        )
    except KeyError as e:
        raise ArgumentException("Missing S3 Bucket environment variable") from e

    files = unpack_filenames(**event)
    archive_files = []
    error_files = []

    try:
        config_type = ConfigType.from_filename(files[0])
        process_logger.add_metadata(config_type=str(config_type))
        process_logger.log_start()
        converter = get_converter(config_type)

        for s3_prefix, table in converter.convert(files):
            write_parquet_file(
                table=table,
                filetype=s3_prefix,
                s3_path=os.path.join(export_bucket, s3_prefix),
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

        # if unable to determine config from filename, or not implemented yet,
        # all files are marked as failed ingestion
        archive_files = []
        error_files = files

    finally:
        if len(error_files) > 0:
            move_s3_objects(error_files, error_bucket)

        if len(archive_files) > 0:
            move_s3_objects(archive_files, archive_bucket)


def lambda_handler(
    event: LambdaDict, context: LambdaContext  # pylint: disable=W0613
) -> None:
    """
    AWS Lambda Python handled function as described in AWS Developer Guide:
    https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
    :param event: The event dict sent by Amazon API Gateway that contains all of
            the request data.
    :param context: The context in which the function is called.
    :return: A response that is sent to Amazon API Gateway, to be wrapped into
             an HTTP response. The 'statusCode' field is the HTTP status code
             and the 'body' field is the body of the response.

    expected event structure is
    {
        prefix: "common_prefix_to_all_files",
        suffix: "common_suffix_to_all_files",
        filespaths: [
            "unique_1",
            "unique_2",
            ...
            "unique_n"
        ]
    }
    where S3 files will begin with 's3://'

    batch files should all be of same ConfigType as each run of this script
    creates a single parquet file.
    """
    logging.info("ingestion_event=%s", json.dumps(event))
    process_logger = ProcessLogger("ingest_files_lambda")

    try:
        main(event, process_logger)
        process_logger.log_complete()
    except Exception as exception:
        process_logger.log_failure(exception)


if __name__ == "__main__":
    load_environment()
    parsed_events = parse_args(sys.argv[1:])
    empty_context = LambdaContext()

    if isinstance(parsed_events, list):
        for parsed_event in parsed_events:
            lambda_handler(parsed_event, empty_context)
    elif isinstance(parsed_events, dict):
        lambda_handler(parsed_events, empty_context)
    else:
        raise Exception("parsed event is not a lambda dict")
