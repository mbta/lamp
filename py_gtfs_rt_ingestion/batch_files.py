#!/usr/bin/env python

import argparse
import sys

from py_gtfs_rt_ingestion import batch_files

DESCRIPTION = "Generate batches of json files that should be processesd"

def parseArgs(args) -> dict:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        '--input',
        dest='input_file',
        type=str,
        required=True,
        help='text file of AWS S3 ls call')

    parser.add_argument(
        '--threshold',
        dest='filesize_threshold',
        type=int,
        required=True,
        default=100000,
        help='filesize threshold for batch sizes')

    parsed_args = parser.parse_args(args)

    return parsed_args

def file_list_from_file(input_file):
    """
    take a file that is the output of `aws s3 ls <dir>` and yield out objects
    one at a time
    """
    for line in open(input_file):
        yield line

def make_file_list(args):
    if args.input_file:
        return file_list_from_file(args.input_file)

    if args.s3_filepath:
        raise Exception("haven't impl'ed yet")

    raise Exception("shouldn't get here")


if __name__ == '__main__':
    args = parseArgs(sys.argv[1:])
    file_list = make_file_list(args)
    batches = batch_files(file_list, args.filesize_threshold)
    for batch in batches:
        print(batch)
