import json
import logging
import os

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import IO, cast

import boto3


def get_s3_client() -> boto3.client:
    """Thin function needed for stubbing tests"""
    return boto3.client("s3")


def get_zip_buffer(filename: str) -> tuple[IO[bytes], int]:
    """
    Get a buffer for a zip file from s3 so that it can be read by zipfile
    module. filename is assumed to be the full path to the zip file without the
    s3:// prefix. Return it along with the last modified date for this s3
    object.
    """
    # inspired by
    # https://betterprogramming.pub/unzip-and-gzip-incoming-s3-files-with-aws-lambda-f7bccf0099c9
    (bucket, file) = filename.split("/", 1)
    s3_resource = boto3.resource("s3")
    zipped_file = s3_resource.Object(bucket_name=bucket, key=file)

    return (
        BytesIO(zipped_file.get()["Body"].read()),
        zipped_file.get()["LastModified"].timestamp(),
    )


def file_list_from_s3(
    bucket_name: str, file_prefix: str
) -> Iterable[tuple[str, int]]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :yield filename, filesize tuples from inside of the bucket
    """
    logging.info(
        "Getting files with prefix %s from %s", file_prefix, bucket_name
    )
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=file_prefix)
    for page in pages:
        if page["KeyCount"] == 0:
            continue
        for obj in page["Contents"]:
            uri = os.path.join("s3://", bucket_name, obj["Key"])
            logging.debug(uri)
            yield (uri, obj["Size"])


def invoke_async_lambda(function_arn: str, event: dict) -> None:
    """
    Invoke a lambda method asynchronously.

    :param function_arn: Lambda function's Amazon Resource Name
    :param event: the event information passed into the invocation of the lambda
                  function

    No return value
    """
    lambda_client = boto3.client("lambda")
    logging.info("Invoking Lambda: %s", function_arn)
    lambda_client.invoke(
        FunctionName=function_arn,
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def _move_s3_object(filename: str, destination: str) -> None:
    # filename is expected as 's3://my_bucket/the/path/to/the/file.json
    try:
        s3_client = get_s3_client()
        logging.info("Moving %s to %s", filename, destination)

        # trim off leading s3://
        copy_key = filename.replace("s3://", "")
        logging.debug("copy key 0: %s", copy_key)

        # string before first delimiter is the bucket name
        source = copy_key.split("/")[0]
        logging.debug("source %s", source)

        # trim off bucket name
        copy_key = copy_key.replace(f"{source}/", "")
        logging.debug("copy key 1: %s", copy_key)

        # json args for cop
        copy_source = {
            "Bucket": source,
            "Key": copy_key,
        }
        logging.debug("Copying")
        s3_client.copy(copy_source, destination, copy_key)

        # delete the source object
        logging.debug("Deleting")
        s3_client.delete_object(**copy_source)
    except Exception as e:
        logging.error("Unable to move %s to %s", filename, destination)
        logging.exception(e)
    else:
        logging.info("Moved %s to %s", filename, destination)


def move_s3_objects(file_list: list[str], destination: str) -> None:
    """
    Move list of S3 objects from source to destination.

    :param file_list: list of s3 filepath uris
    :param destination: directory or S3 bucket to move to formatted without
        leading 's3://'

    No return value.
    """
    destination = destination.split("/")[0]

    # this is the default pool size for a ThreadPoolExecutor as of py3.8
    cpu_count = cast(int, os.cpu_count() if os.cpu_count() is not None else 1)
    pool_size = min(32, cpu_count + 4)

    logging.info(
        "Moving %s files to %s using %d threads",
        len(file_list),
        destination,
        pool_size,
    )

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        for filename in file_list:
            executor.submit(_move_s3_object, filename, destination)
