#!/usr/bin/env python

import argparse
import json
import sys

from collections.abc import Iterable
from typing import NamedTuple

from py_gtfs_rt_ingestion import batch_files, file_list_from_s3

import logging
logging.basicConfig(level=logging.INFO)

DESCRIPTION = "Generate batches of json files that should be processed"

class BatchArgs(NamedTuple):
    input_file: str
    s3_prefix: str
    filesize_threshold: int
    output: str
    pretty: bool

def parseArgs(args) -> BatchArgs:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--input',
        dest='input_file',
        type=str,
        help='text file of AWS S3 ls call')

    parser.add_argument(
        '--s3-prefix',
        dest='s3_prefix',
        type=str,
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

    parser.add_argument(
        '--output',
        dest='output',
        type=str,
        help='filepath to dump out batches as event json list')

    parsed_args = BatchArgs(**vars(parser.parse_args(args)))

    return parsed_args

def file_list_from_file(input_file: str) -> Iterable[(str, int)]:
    """
    take a file that is the output of `aws s3 ls <dir>` and yield out filename,
    filesize tuples.
    """
    for file_info in open(input_file):
        (date, time, size, filename) = file_info.split()
        yield (filename, size)

def make_file_list(args: BatchArgs) -> Iterable[(str, int)]:
    """
    based on args, yield out filename, filesize tuples either from a local file
    or from our s3 bucket.
    """
    if args.input_file:
        return file_list_from_file(args.input_file)

    if args.s3_prefix:
        return file_list_from_s3(file_prefix=args.s3_prefix)

    raise Exception("shouldn't get here")

if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    file_list = make_file_list(args)
    batches = batch_files(file_list, args.filesize_threshold)

    if args.pretty:
        total_bytes = 0
        total_files = 0
        for batch in batches:
            total_bytes += batch.total_size
            total_files += len(batch.filenames)

        total_gigs = total_bytes / 1000000000
        logging.info("Batched %d gigs across %d files" % (total_gigs,
                                                          total_files))

    if args.output:
        with open(args.output, 'w') as output_file:
            json.dump([b.create_event() for b in batches],
                      output_file,
                      indent=2)
