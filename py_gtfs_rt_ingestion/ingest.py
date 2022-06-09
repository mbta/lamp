#!/usr/bin/env python

import argparse
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import sys

from concurrent.futures import ThreadPoolExecutor
from pyarrow import fs

from py_gtfs_rt_ingestion import ArgumentException
from py_gtfs_rt_ingestion import Configuration
from py_gtfs_rt_ingestion import ConfigTypeFromFilenameException
from py_gtfs_rt_ingestion import NoImplException
from py_gtfs_rt_ingestion import DEFAULT_S3_PREFIX
from py_gtfs_rt_ingestion import gz_to_pyarrow
from py_gtfs_rt_ingestion import move_s3_objects

import logging
logging.getLogger().setLevel('INFO')

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."

# TODO this is fine for now, but maybe an environ variable?
POOL_SIZE = 4

def parseArgs(args) -> dict:
    """
    parse input args from the command line and generate an event dict in the
    format the lambda handler is expecting
    """
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--input',
        dest='input_file',
        type=str,
        help='provide filename to ingest')

    parser.add_argument(
        '--event-json',
        dest='event_json',
        type=str,
        help='lambda event json file')

    parser.add_argument(
        '--export',
        dest='export_dir',
        type=str,
        help='where to export to')

    parser.add_argument(
        '--archive',
        dest='archive_dir',
        type=str,
        help='where to archive ingested files to')

    parser.add_argument(
        '--error',
        dest='error_dir',
        type=str,
        help='where to move unconverted files to')

    parsed_args = parser.parse_args(args)

    if parsed_args.export_dir is not None:
        os.environ['EXPORT_BUCKET'] = parsed_args.export_dir
    if parsed_args.archive_dir is not None:
        os.environ['ARCHIVE_BUCKET'] = parsed_args.archive_dir
    if parsed_args.error_dir is not None:
        os.environ['ERROR_BUCKET'] = parsed_args.error_dir

    if parsed_args.event_json is not None:
        with open(parsed_args.event_json) as event_json_file:
            events = json.load(event_json_file)

        return events

    return {
        'files': [(parsed_args.input_file)]
    }

def main(files: list[str]) -> None:
    try:
        EXPORT_BUCKET = os.path.join(os.environ['EXPORT_BUCKET'],
                                     DEFAULT_S3_PREFIX)
        ARCHIVE_BUCKET = os.path.join(os.environ['ARCHIVE_BUCKET'],
                                      DEFAULT_S3_PREFIX)
        ERROR_BUCKET = os.path.join(os.environ['ERROR_BUCKET'],
                                    DEFAULT_S3_PREFIX)
    except KeyError as e:
        raise ArgumentException("Missing S3 Bucket environment variable") from e

    logging.info(
        "Creating pool with %d threads, %d cores available" % (POOL_SIZE,
                                                               os.cpu_count()))

    # list of files to move to error bucket to be inspected and processed later
    failed_ingestion = []

    try:
        config = Configuration(filename=files[0])

        pa_table = pa.table(config.empty_table(), schema=config.export_schema)

        with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
            for result in executor.map(lambda x: gz_to_pyarrow(*x),
                                       [(f, config) for f in files]):
                if isinstance(result, pa.Table):
                    pa_table = pa.concat_tables([pa_table, result])
                else:
                    failed_ingestion.append(result)

        logging.info(
            "Completed converting %d files with config %s" % (len(files),
                                                              config.config_type))

        logging.info("Writing Table to %s" % EXPORT_BUCKET)
        s3 = fs.S3FileSystem()
        pq.write_to_dataset(
            table=pa_table,
            root_path=os.path.join(EXPORT_BUCKET, str(config.config_type)),
            filesystem=s3,
            partition_cols=['year','month','day','hour'],
        )

    except (ConfigTypeFromFilenameException, NoImplException) as e:
        # if unable to determine config from filename, or not implemented yet,
        # all files are marked as failed ingestion
        logging.error(e)
        failed_ingestion = files

    if len(failed_ingestion) > 0:
        logging.warning("Unable to process %d files" % len(failed_ingestion))
        move_s3_objects(failed_ingestion, ERROR_BUCKET)

    files_to_archive = list(set(files) - set(failed_ingestion))
    move_s3_objects(files_to_archive, ARCHIVE_BUCKET)

def lambda_handler(event: dict, context) -> None:
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
    logging.info("Processing event:\n%s" % json.dumps(event, indent=2))

    try:
        files = event['files']
        main(files)
    except Exception as e:
        # log if something goes wrong and let lambda recatch the exception
        logging.exception(e)
        raise e

if __name__ == '__main__':
    event = parseArgs(sys.argv[1:])

    if type(event) == list:
        for e in event:
            lambda_handler(e, {})
    else:
        lambda_handler(event, {})
