import json
import os
import pathlib

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import IO, List, Tuple, cast

from pyarrow import fs, Table
from pyarrow.util import guid
import boto3
import pyarrow.dataset as ds

from .logging_utils import ProcessLogger
from .postgres_utils import insert_metadata


def get_s3_client() -> boto3.client:
    """Thin function needed for stubbing tests"""
    return boto3.client("s3")


def get_zip_buffer(filename: str) -> Tuple[IO[bytes], int]:
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


def file_list_from_s3(bucket_name: str, file_prefix: str) -> List[str]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :return list of s3 filepaths sorted by the timestamps formatted into them
    """
    process_logger = ProcessLogger(
        "file_list_from_s3", bucket_name=bucket_name, file_prefix=file_prefix
    )
    process_logger.log_start()

    def strip_timestamp(fileobject: str) -> str:
        """
        utility for sorting pulling timestamp string out of file path.
        assumption is that the objects will have a bunch of "directories" that
        pathlib can parse out, and the filename will start with a timestamp
        "YYY-MM-DDTHH:MM:SSZ" (20 char) format.

        This utility will be used to sort the list of objects.
        """
        filepath = pathlib.Path(fileobject)
        return filepath.name[:20]

    try:
        s3_client = get_s3_client()
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=file_prefix)

        filepaths = []
        for page in pages:
            if page["KeyCount"] == 0:
                continue
            for obj in page["Contents"]:
                filepaths.append(os.path.join("s3://", bucket_name, obj["Key"]))

        filepaths.sort(key=strip_timestamp)
        process_logger.log_complete()
        return filepaths
    except Exception as exception:
        process_logger.log_failure(exception)
        return []


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
    process_logger = ProcessLogger(
        "move_s3_object",
        filename=filename,
        destination=destination,
    )
    process_logger.log_start()

    try:
        session = boto3.session.Session()
        s3_resource = session.resource("s3")

        # trim off leading s3://
        copy_key = filename.replace("s3://", "")

        # string before first delimiter is the bucket name
        source = copy_key.split("/")[0]

        # trim off bucket name
        copy_key = copy_key.replace(f"{source}/", "")

        # json args for cop
        destination_bucket = s3_resource.Bucket(destination)
        destination_object = destination_bucket.Object(copy_key)
        destination_object.copy(
            {
                "Bucket": source,
                "Key": copy_key,
            }
        )

        # delete the source object
        source_bucket = s3_resource.Bucket(source)
        source_object = source_bucket.Object(copy_key)
        source_object.delete()

        process_logger.log_complete()
    except Exception as exception:
        process_logger.log_failure(exception)


def move_s3_objects(files: List[str], destination: str) -> None:
    """
    Move list of S3 objects from source to destination.

    :param files: list of s3 filepath uris
    :param destination: directory or S3 bucket to move to formatted without
        leading 's3://'

    No return value.
    """
    destination = destination.split("/")[0]

    # this is the default pool size for a ThreadPoolExecutor as of py3.8
    cpu_count = cast(int, os.cpu_count() if os.cpu_count() is not None else 1)
    pool_size = min(32, cpu_count + 4)

    process_logger = ProcessLogger(
        "move_s3_objects",
        pool_size=pool_size,
        destination=destination,
        file_count=len(files),
    )
    process_logger.log_start()

    with ThreadPoolExecutor(max_workers=pool_size) as executor:
        for filename in files:
            executor.submit(_move_s3_object, filename, destination)

    process_logger.log_complete()


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
    process_logger = ProcessLogger(
        "write_parquet", filetype=filetype, number_of_rows=table.num_rows
    )
    process_logger.log_start()

    # generate partitioning for this table write based on what columns
    # we expect to be able to partition out for this input type
    partitioning = ds.partitioning(
        table.select(partition_cols).schema, flavor="hive"
    )

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

    process_logger.log_complete()
