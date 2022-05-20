#!/usr/bin/env python

import argparse
import boto3
import json
import sys

from collections.abc import Iterable

from py_gtfs_rt_ingestion import batch_files, file_list_from_s3

DESCRIPTION = "Generate batches of json files that should be processesd"

def parseArgs(args) -> dict:
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
        '--output',
        dest='output_dir',
        type=str,
        required=True,
        help='provide a directory to output')

    parsed_args = parser.parse_args(args)

    return parsed_args

def file_list_from_file(input_file: str) -> Iterable[(str, int)]:
    """
    take a file that is the output of `aws s3 ls <dir>` and yield out filename,
    filesize tuples.
    """
    for file_info in open(input_file):
        (date, time, size, filename) = file_info.split()
        yield (filename, sie)

def make_file_list(args: dict) -> Iterable[(str, int)]:
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

    print(json.dumps([b.create_event() for b in batches], indent=2))
