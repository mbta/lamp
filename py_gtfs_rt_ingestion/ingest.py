#!/usr/bin/env python

import argparse
import json
import os
import pyarrow.parquet as pq
import sys
import tempfile

from pathlib import Path

from py_gtfs_rt_ingestion import (Configuration,
                                  convert_files,
                                  download_file_from_s3)

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."

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

    parsed_args = parser.parse_args(args)

    if parsed_args.output_dir is not None:
        os.environ['OUTPUT_DIR'] = parsed_args.output_dir

    if parsed_args.event_json is not None:
        with open(parsed_args.event_json) as event_json_file:
            events = json.load(event_json_file)

        return events

    return {
        'files': [Path(parsed_args.input_file)]
    }

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

    expected event structure is either
    {
        files: [file_name_1, file_name_2, ...],
    }

    or
    {
        s3_files: [file_name_1, file_name_2, ...],
        bucket: bucket_name
    }

    batch files should all be of same ConfigType
    """
    logging.info("Processing event:\n%s" % json.dumps(event, indent=2))
    temp_dir = None
    if 'files' not in event:
        temp_dir = tempfile.TemporaryDirectory()
        files = [download_file_from_s3(event['bucket'], file, temp_dir.name)
                 for file in event['s3_files']]

    else:
        files = event['files']

    config = Configuration(filename=files[0].name)

    pa_table = convert_files(files, config)

    if pa_table is not None:
        logging.info("Writing Table for %s" % config.config_type)
        pq.write_to_dataset(
            pa_table,
            root_path=os.environ['OUTPUT_DIR'],
            partition_cols=['year','month','day','hour']
        )

if __name__ == '__main__':
    event = parseArgs(sys.argv[1:])

    if type(event) == list:
        for e in event:
            lambda_handler(e, {})
    else:
        lambda_handler(event, {})
