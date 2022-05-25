#!/usr/bin/env python

import argparse
import json
import os
import pyarrow as pa
import sys

from multiprocessing import Pool

from py_gtfs_rt_ingestion import Configuration, gz_to_pyarrow, s3_to_pyarrow

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Convert a json file into a parquet file. Used for testing."

# TODO this is fine for now, but maybe an environ variable?
MULTIPROCESSING_POOL_SIZE = 4

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
        'files': [(parsed_args.input_file)]
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

    # get files and function to read them based on the event. for local files,
    # use gzip reading, for s3 files use pyarrow to read directly from s3
    if 'files' in event:
        files = event['files']
        conversion_func = gz_to_pyarrow
    elif 's3_files' in event:
        files = event['s3_files']
        conversion_func = s3_to_pyarrow
    else:
        raise Exception("poorly formatted event")

    config = Configuration(filename=files[0])

    logging.info("Creating pool with %d threads" % MULTIPROCESSING_POOL_SIZE)

    pool = Pool(MULTIPROCESSING_POOL_SIZE)
    workers = pool.starmap_async(conversion_func, [(f, config) for f in files])

    pa_table = pa.table(config.empty_table(), schema=config.export_schema)
    failed_ingestion = []

    for result in workers.get():
        if isinstance(result, pa.Table):
            if pa_table is not None:
                pa_table = pa.concat_tables([pa_table, result])
            else:
                pa_table = result
        else:
            failed_ingestion.append(result)

    logging.info(
        "Completed converting %d files with config %s" % (len(files),
                                                          config.config_type))

    if len(failed_ingestion) > 0:
        logging.warning("Unable to process %d files" % len(failed_ingestion))

    return (pa_table, failed_ingestion)

    logging.info("Writing Table to %s" % os.environ['OUTPUT_DIR'])
    pa.parquet.write_to_dataset(
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
