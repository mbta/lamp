#!/usr/bin/env python

import argparse
import json
import os
import sys

from collections.abc import Iterable
from typing import NamedTuple

from py_gtfs_rt_ingestion import batch_files
from py_gtfs_rt_ingestion import file_list_from_s3

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Generate batches of json files that should be processed"

class BatchArgs(NamedTuple):
    s3_prefix: str
    s3_bucket: str
    filesize_threshold: int
    print_events: bool = False
    dry_run: bool = False

def parseArgs(args) -> dict:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--s3-prefix',
        dest='s3_prefix',
        type=str,
        default='',
        help='prefix to files in the mbta-gtfs-s3 bucket')

    parser.add_argument(
        '--s3-bucket',
        dest='s3_bucket',
        type=str,
        default='mbta-gtfs-s3',
        help='prefix to files in the mbta-gtfs-s3 bucket')

    parser.add_argument(
        '--threshold',
        dest='filesize_threshold',
        type=int,
        default=100000,
        help='filesize threshold for batch sizes')

    parser.add_argument(
        '--dry-run',
        dest='dry_run',
        action='store_true',
        help='do not invoke ingest lambda function')

    parser.add_argument(
        '--print-events',
        dest='print_events',
        action='store_true',
        help='print out each event as json to stdout')

    return vars(parser.parse_args(args))

def main(batch_args: BatchArgs) -> None:
    file_list = file_list_from_s3(bucket_name=batch_args.s3_bucket,
                                  file_prefix=batch_args.s3_prefix)


    total_bytes = 0
    total_files = 0
    events = []
    for batch in batch_files(file_list, batch_args.filesize_threshold):
        total_bytes += batch.total_size
        total_files += len(batch.filenames)
        events.append(batch.create_event())

        if not batch_args.dry_run:
            try:
                batch.trigger_lambda()
            except Exception as e:
                logging.exception("Unable to trigger ingest lambda.\n%s" % e)

    total_gigs = total_bytes / 1000000000
    logging.info("Batched %d gigs across %d files" % (total_gigs,
                                                      total_files))

    if batch_args.print_events:
        print(json.dumps(events, indent=2))

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
        s3_prefix: str
        s3_bucket: str
        filesize_threshold: int
    }

    batch files should all be of same ConfigType
    """
    logging.info("Processing event:\n%s" % json.dumps(event, indent=2))
    logging.info("Context:\n%s" % json.dumps(context, indent=2))
    try:
        batch_args = BatchArgs(**event)
        logging.info(batch_args)
        main(batch_args)
    except Exception as e:
        # log if something goes wrong and let lambda recatch the exception
        logging.exception(e)
        raise e

if __name__ == '__main__':
    event = parseArgs(sys.argv[1:])
    lambda_handler(event, None)

