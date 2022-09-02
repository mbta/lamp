import json
import logging
import os
import time

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import IO, List, cast

from pyarrow import fs, Table
from pyarrow.util import guid
import boto3
import pyarrow.dataset as ds

from .postgres_utils import insert_metadata


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
    lambda_client.invoke(
        FunctionName=function_arn,
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def _move_s3_object(filename: str, destination: str) -> None:
    """
    move a single s3 file to the destination bucket. break the incoming s3 path
    intco parts that are used for copying. each execution will spin up a session
    and get a resource from that session to avoid threading issues.

    :param filename - expected as 's3://my_bucket/the/path/to/the/file.json
    :param destination bucket name
    """
    start_time = time.time()
    logging.info(
        "start=%s, filename=%s, destination=%s",
        "move_s3_object",
        filename,
        destination,
    )

    try:
        session = boto3.session.Session()
        s3_resource = session.resource("s3")

        # trim off leading s3://
        copy_key = filename.replace("s3://", "")
        logging.debug("copy_key_0=%s", copy_key)

        # string before first delimiter is the bucket name
        source = copy_key.split("/")[0]
        logging.debug("source=%s", source)

        # trim off bucket name
        copy_key = copy_key.replace(f"{source}/", "")
        logging.debug("copy_key_1=%s", copy_key)

        # json args for cop
        destination_bucket = s3_resource.Bucket(destination)
        destination_object = destination_bucket.Object(copy_key)
        logging.debug("Copying")
        destination_object.copy(
            {
                "Bucket": source,
                "Key": copy_key,
            }
        )

        # delete the source object
        source_bucket = s3_resource.Bucket(source)
        source_object = source_bucket.Object(copy_key)
        logging.debug("Deleting")
        source_object.delete()
    except Exception as exception:
        logging.exception(
            "failed=%s, error_type=%s, filename=%s, destination=%s",
            "move_s3_object",
            type(exception).__name__,
            filename,
            destination,
        )
    else:
        logging.info(
            "start=%s, duration=%.2f, filename=%s, destination=%s",
            "move_s3_object",
            start_time - time.time(),
            filename,
            destination,
        )


def move_s3_objects(file_list: list[str], destination: str) -> None:
    """
    Move list of S3 objects from source to destination.

    :param file_list: list of s3 filepath uris
    :param destination: directory or S3 bucket to move to formatted without
        leading 's3://'

    No return value.
    """
    start_time = time.time()
    destination = destination.split("/")[0]

    # this is the default pool size for a ThreadPoolExecutor as of py3.8
    cpu_count = cast(int, os.cpu_count() if os.cpu_count() is not None else 1)
    pool_size = min(32, cpu_count + 4)

    logging.info(
        "start=%s, pool_size=%s, destination=%s, file_count=%s",
        "move_s3_objects",
        pool_size,
        destination,
        len(file_list),
    )

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        for filename in file_list:
            executor.submit(_move_s3_object, filename, destination)

    logging.info(
        "complete=%s, duration=%s", "move_s3_objects", start_time - time.time()
    )


def write_parquet_file(
    table: Table, filetype: str, s3_path: str, partition_cols: List[str]
) -> None:
    """
    Helper function to write out a parquet table to an s3 path, patitioning
    based on columns. As files are written, add them to the metadata table of
    the performance manager database.

    This method mostly duplicates the function pyarrow.parquet.write_table that
    we were using in earlier versions of the ingestion script. Unfortunately,
    that method has a bug around the `file_visitor` argument, which we need to
    write to the metadata table. We use the same defaults here that are chosen
    in that file.

    It appears that this bug isn't going to be fixed and using the
    dataset.write_dataset is the preferred method for writing parquet files
    going forward. https://issues.apache.org/jira/browse/ARROW-17068
    """
    # generate partitioning for this table write based on what columns
    # we expect to be able to partition out for this input type
    partitioning = ds.partitioning(
        table.select(partition_cols).schema, flavor="hive"
    )

    logging.info(
        "start=%s, filetype=%s, number_of_rows=%d",
        "write_parquet",
        filetype,
        table.num_rows,
    )
    write_start_time = time.time()

    ds.write_dataset(
        data=table,
        base_dir=s3_path,
        filesystem=fs.S3FileSystem(),
        format=ds.ParquetFileFormat(),
        partitioning=partitioning,
        file_visitor=insert_metadata,
        basename_template=guid() + "-{i}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    logging.info(
        "comple=%s, duration=%.2f",
        "write_parquet",
        time.time() - write_start_time,
    )
