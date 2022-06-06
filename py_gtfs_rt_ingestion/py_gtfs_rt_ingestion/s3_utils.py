import boto3
import logging

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
