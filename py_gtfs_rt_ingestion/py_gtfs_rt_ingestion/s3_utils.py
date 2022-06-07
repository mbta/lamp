import boto3
import json
import logging
import re

from collections.abc import Iterable

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
    pages = paginator.paginate(
        Bucket=bucket_name,
        Prefix=file_prefix,
    )
    for page in pages:
        if page['KeyCount'] == 0:
            continue
        for obj in page['Contents']:
            yield (obj['Key'], obj['Size'])

def invoke_async_lambda(function_arn: str, event: dict) -> None:
    lambda_client = boto3.client('lambda')
    logging.info("Invoking Lambda: %s" % function_arn)
    lambda_client.invoke(FunctionName=function_arn,
                         InvocationType='Event',
                         Payload=json.dumps(event))

def move_3s_objects(obj_list: list[str], 
                    src_bucket: str,
                    dest_bucket: str) -> None:
    """
    Move list of S3 objects from src_bucket to dest_bucket.

    :param obj_list: list of S3 object paths (paths must start with src_bucket)
    :param src_bucket: S3 bucket to move from
    :param dest_bucket: S3 bucket to move to

    No return value.
    """
    s3_client = get_s3_client()
    for obj in obj_list:
        # Check if source bucket is in obj path, should skip local files???
        if src_bucket not in obj:
            logging.warning("%s bucket not found in %s path" % (src_bucket, 
                                                             obj))
            continue
        # strip 's3://' and bucket name from obj path
        copy_key = re.sub(rf"({src_bucket}/|s3://)","",obj)
        copy_source = {
                'Bucket': src_bucket,
                'Key': copy_key,
            }
        try:
            s3_client.copy(copy_source, dest_bucket, copy_key)
            s3_client.delete(copy_source)
        except Exception as e:
            logging.exception(e)
        else:
            logging.info("Moved %s from %s to %s" % (obj,
                                                     src_bucket,
                                                     dest_bucket))
