#!/usr/bin/env python

import argparse
import json
import sys

from collections.abc import Iterable
from typing import NamedTuple

from py_gtfs_rt_ingestion import ArgumentException
from py_gtfs_rt_ingestion import batch_files
from py_gtfs_rt_ingestion import file_list_from_s3

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Generate batches of json files that should be processed"

class BatchArgs(NamedTuple):
    s3_prefix: str
    s3_bucket: str
    filesize_threshold: int
    pretty: bool

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
        '--pretty',
        dest='pretty',
        action='store_true',
        help='print out a pretty summary of the batches')

    return vars(parser.parse_args(args))

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
        pretty: bool
    }

    batch files should all be of same ConfigType
    """
    logging.info("Processing event:\n%s" % json.dumps(event, indent=2))
    batch_args = BatchArgs(**event)
    logging.info(batch_args)

    file_list = file_list_from_s3(bucket_name=batch_args.s3_bucket,
                                  file_prefix=batch_args.s3_prefix)

    batches = batch_files(file_list, batch_args.filesize_threshold)

    # TODO use boto3 to launch ingestion jobs from each batch

    # log out a summary of whats been batched.
    if batch_args.pretty:
        total_bytes = 0
        total_files = 0
        for batch in batches:
            total_bytes += batch.total_size
            total_files += len(batch.filenames)

        total_gigs = total_bytes / 1000000000
        logging.info("Batched %d gigs across %d files" % (total_gigs,
                                                          total_files))
    else:
        print(json.dumps([b.create_event() for b in batches], indent=2))

if __name__ == '__main__':
    event = parseArgs(sys.argv[1:])
    lambda_handler(event, None)

