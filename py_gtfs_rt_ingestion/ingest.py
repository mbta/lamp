#!/usr/bin/env python

import argparse
import json
import logging
import os
import sys

from concurrent.futures import ThreadPoolExecutor
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq

from pyarrow import fs

from py_gtfs_rt_ingestion import ArgumentException
from py_gtfs_rt_ingestion import ConfigTypeFromFilenameException
from py_gtfs_rt_ingestion import Configuration
from py_gtfs_rt_ingestion import ConfigType
from py_gtfs_rt_ingestion import DEFAULT_S3_PREFIX
from py_gtfs_rt_ingestion import LambdaContext
from py_gtfs_rt_ingestion import LambdaDict
from py_gtfs_rt_ingestion import NoImplException
from py_gtfs_rt_ingestion import gz_to_pyarrow
from py_gtfs_rt_ingestion import zip_to_pyarrow
from py_gtfs_rt_ingestion import move_s3_objects

logging.getLogger().setLevel("INFO")

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."
POOL_SIZE = 4


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

    parser.add_argument(
        "--export", dest="export_dir", type=str, help="where to export to"
    )

    parser.add_argument(
        "--archive",
        dest="archive_dir",
        type=str,
        help="where to archive ingested files to",
    )

    parser.add_argument(
        "--error",
        dest="error_dir",
        type=str,
        help="where to move unconverted files to",
    )

    parsed_args = parser.parse_args(args)

    if parsed_args.export_dir is not None:
        os.environ["EXPORT_BUCKET"] = parsed_args.export_dir
    if parsed_args.archive_dir is not None:
        os.environ["ARCHIVE_BUCKET"] = parsed_args.archive_dir
    if parsed_args.error_dir is not None:
        os.environ["ERROR_BUCKET"] = parsed_args.error_dir

    if parsed_args.event_json is not None:
        with open(parsed_args.event_json, encoding="utf8") as event_json_file:
            events: dict = json.load(event_json_file)

        return events

    return {"files": [(parsed_args.input_file)]}


def main(files: list[str]) -> None:
    """
    * Convert a list of json files from s3 to a parquet table
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

    # list of files to move to error bucket to be inspected and processed later
    failed_ingestion = []

    # filesystem to use when writing parquet files
    s3_filesystem = fs.S3FileSystem()

    try:
        config_type = ConfigType.from_filename(files[0])
        if config_type == ConfigType.SCHEDULE:
            if len(files) != 1:
                raise ArgumentException(
                    "Received more than one schedule GTFS zip file"
                )
            logging.info("Reading %s file and converting to parquet", files[0])
            for prefix, table in zip_to_pyarrow(files[0]):
                pq.write_to_dataset(
                    table=table,
                    root_path=os.path.join(export_bucket, prefix),
                    filesystem=s3_filesystem,
                    partition_cols=["timestamp"],
                )
        else:
            config = Configuration(config_type)
            pa_table = pa.table(
                config.empty_table(), schema=config.export_schema
            )

            logging.info(
                "Creating pool with %d threads, %d cores available",
                POOL_SIZE,
                os.cpu_count(),
            )

            with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
                for result in executor.map(
                    lambda x: gz_to_pyarrow(*x), [(f, config) for f in files]
                ):
                    if isinstance(result, pa.Table):
                        pa_table = pa.concat_tables([pa_table, result])
                    else:
                        failed_ingestion.append(result)

            logging.info(
                "Completed converting %d files with config %s",
                len(files),
                config.config_type,
            )

            logging.info("Writing Table to %s", export_bucket)
            pq.write_to_dataset(
                table=pa_table,
                root_path=os.path.join(export_bucket, str(config.config_type)),
                filesystem=s3_filesystem,
                partition_cols=["year", "month", "day", "hour"],
            )

    except (ConfigTypeFromFilenameException, NoImplException) as e:
        # if unable to determine config from filename, or not implemented yet,
        # all files are marked as failed ingestion
        logging.error(e)
        failed_ingestion = files

    if len(failed_ingestion) > 0:
        logging.warning("Unable to process %d files", len(failed_ingestion))
        move_s3_objects(failed_ingestion, error_bucket)

    files_to_archive = list(set(files) - set(failed_ingestion))
    move_s3_objects(files_to_archive, archive_bucket)


def lambda_handler(event: LambdaDict, context: LambdaContext) -> None:
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
        files: [file_name_1, file_name_2, ...],
    }
    where S3 files will begin with 's3://'

    batch files should all be of same ConfigType as each run of this script
    creates a single parquet file.
    """
    logging.info("Processing event:\n%s", json.dumps(event, indent=2))
    logging.info("Context:%s", context)

    try:
        files = event["files"]
        main(files)
    except Exception as exception:
        # log if something goes wrong and let lambda recatch the exception
        logging.exception(exception)
        raise exception


if __name__ == "__main__":
    parsed_events = parse_args(sys.argv[1:])
    empty_context = LambdaContext()

    if isinstance(parsed_events, list):
        for parsed_event in parsed_events:
            lambda_handler(parsed_event, empty_context)
    elif isinstance(parsed_events, dict):
        lambda_handler(parsed_events, empty_context)
    else:
        raise Exception("parsed event is not a lambda dict")
