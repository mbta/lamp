import boto3
import json
import logging
import os
import re

from collections.abc import Iterable
from pyarrow import fs

def get_s3_client():
    return boto3.client('s3')

def file_list_from_s3(bucket_name: str,
                      file_prefix: str) -> Iterable[(str, int)]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :yield filename, filesize tuples from inside of the bucket
    """
    logging.info("Getting files with prefix %s from %s" % (file_prefix,
                                                           bucket_name))
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=file_prefix)
    for page in pages:
        if page['KeyCount'] == 0:
            continue
        for obj in page['Contents']:
            uri = os.path.join('s3://', bucket_name, obj['Key'])
            logging.info(uri)
            yield (uri, obj['Size'])

def invoke_async_lambda(function_arn: str, event: dict) -> None:
    lambda_client = boto3.client('lambda')
    logging.info("Invoking Lambda: %s" % function_arn)
    lambda_client.invoke(FunctionName=function_arn,
                         InvocationType='Event',
                         Payload=json.dumps(event))

def move_s3_objects(file_list: list[str],
                    destination: str) -> None:
    """
    Move list of S3 objects from src_bucket to dest_bucket.

    :param file_list: list of s3 filepath uris
    :param dest_bucket: directory or S3 bucket to move to

    No return value.
    """
    file_system = fs.S3FileSystem()
    logging.info("Moving %s files to %s" % (len(file_list), destination))

    for filename in file_list:
        try:
            logging.info("Moving %s to %s" % (filename, destination))
            filename = filename.replace('s3://', '')
            src_path = filename.split("/")
            dest_path = [destination] + src_path[-2:]
            dest_filename = os.path.join(*dest_path)

            file_system.move(src=filename, dest=dest_filename)

        except Exception as e:
            logging.error("Unable to move %s to %s" % (filename, destination))
            logging.exception(e)
        else:
            logging.info("Moved %s to %s" % (filename, destination))
