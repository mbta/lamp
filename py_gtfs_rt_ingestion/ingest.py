#!/usr/bin/env python

import argparse
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import sys

from multiprocessing import Pool
from typing import NamedTuple

from py_gtfs_rt_ingestion import ArgumentException
from py_gtfs_rt_ingestion import Configuration
from py_gtfs_rt_ingestion import gz_to_pyarrow
from py_gtfs_rt_ingestion import move_3s_objects

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."

# TODO this is fine for now, but maybe an environ variable?
MULTIPROCESSING_POOL_SIZE = 4

class IngestArgs(NamedTuple):
    input_file: str
    event_json: str
    output_dir: str

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
        '--output',
        dest='output_dir',
        type=str,
        required=True,
        help='provide a directory to output')

    parsed_args = IngestArgs(**vars(parser.parse_args(args)))

    if parsed_args.output_dir is not None:
        os.environ['OUTPUT_DIR'] = parsed_args.output_dir

    if parsed_args.event_json is not None:
        with open(parsed_args.event_json) as event_json_file:
            events = json.load(event_json_file)

        return events

    return {
        'files': [(parsed_args.input_file)]
    }

def main(files: list[str]) -> None:
    config = Configuration(filename=files[0])

    try:
        INGEST_BUCKET = os.environ['ingest_bucket']
        OUTPUT_BUCKET = os.environ['OUTPUT_DIR']
        ARCHIVE_BUCKET = os.environ['archive_bucket']
        ERROR_BUCKET = os.environ['error_bucket']
    except KeyError as e:
        raise ArgumentException("Missing S3 Bucket environment variable") from e

    logging.info("Creating pool with %d threads" % MULTIPROCESSING_POOL_SIZE)

    pool = Pool(MULTIPROCESSING_POOL_SIZE)
    workers = pool.starmap_async(gz_to_pyarrow, [(f, config) for f in files])

    pa_table = pa.table(config.empty_table(), schema=config.export_schema)
    failed_ingestion = []

    for result in workers.get():
        if isinstance(result, pa.Table):
            pa_table = pa.concat_tables([pa_table, result])
        else:
            failed_ingestion.append(result)

    logging.info(
        "Completed converting %d files with config %s" % (len(files),
                                                          config.config_type))

    if len(failed_ingestion) > 0:
        logging.warning("Unable to process %d files" % len(failed_ingestion))
        move_3s_objects(failed_ingestion, INGEST_BUCKET, ERROR_BUCKET)

    logging.info("Writing Table to %s" % OUTPUT_BUCKET)
    pq.write_to_dataset(
        pa_table,
        root_path=OUTPUT_BUCKET,
        partition_cols=['year','month','day','hour']
    )
    files_to_archive = list(set(files) - set(failed_ingestion))
    move_3s_objects(files_to_archive, INGEST_BUCKET, ARCHIVE_BUCKET)

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
