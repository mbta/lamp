import os
import time

from multiprocessing.pool import Pool
from multiprocessing import current_process
from io import BytesIO
from typing import IO, List, Tuple, cast, Callable, Optional

from pyarrow import fs, Table
from pyarrow.util import guid
import boto3
import pyarrow.dataset as ds

from .logging_utils import ProcessLogger


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

    :return list of s3 filepaths
    """
    process_logger = ProcessLogger(
        "file_list_from_s3", bucket_name=bucket_name, file_prefix=file_prefix
    )
    process_logger.log_start()

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

        process_logger.add_metadata(list_size=len(filepaths))
        process_logger.log_complete()
        return filepaths
    except Exception as exception:
        process_logger.log_failure(exception)
        return []


def _move_s3_object(filename: str, to_bucket: str) -> Optional[str]:
    """
    move a single s3 file to the to_bucket bucket. break the incoming s3 path
    into parts that are used for copying. each process will have it's own
    boto session and resource available, from the _init_process_session function
    to avoid multi-processing issues

    :param filename - expected as 's3://my_bucket/the/path/to/the/file.json
    :param to_bucket bucket name

    :return - 'None' if exception occured during move, otherwise 'filename'
    """
    try:
        s3_resource = current_process().__dict__["boto_s3_resource"]

        # trim off leading s3://
        copy_key = filename.replace("s3://", "")

        # string before first delimiter is the bucket name
        from_bucket = copy_key.split("/")[0]

        # trim off bucket name
        copy_key = copy_key.replace(f"{from_bucket}/", "")

        # json args for cop
        destination_bucket = s3_resource.Bucket(to_bucket)
        destination_object = destination_bucket.Object(copy_key)
        destination_object.copy(
            {
                "Bucket": from_bucket,
                "Key": copy_key,
            }
        )

        # delete the source object
        source_bucket = s3_resource.Bucket(from_bucket)
        source_object = source_bucket.Object(copy_key)
        source_object.delete()

    except Exception as _:
        return None

    return filename


def _init_process_session() -> None:
    """
    initialization function for any process in multi processing pool needing to
    use a boto session object

    this avoids the expensive operation of creating a new session for every unti of work
    in the pool

    not totally sure this is the best way to retain the session on process initialization
    but it seems to work
    """
    process_data = current_process()
    process_data.__dict__["boto_session"] = boto3.session.Session()
    process_data.__dict__["boto_s3_resource"] = process_data.__dict__[
        "boto_session"
    ].resource("s3")


def move_s3_objects(files: List[str], to_bucket: str) -> List[str]:
    """
    Move list of S3 objects to to_bucket bucket, retaining the object path.

    :param files: list of s3 filepath uris
    :param destination: directory or S3 bucket to move to formatted without
        leading 's3://'

    :reutrn - list of 3s objects that failed to move
    """
    to_bucket = to_bucket.split("/")[0]

    files_to_move = set(files)
    found_exception = Exception("No exception reported from s3 move pool.")

    # this is the default pool size for a ThreadPoolExecutor as of py3.8
    cpu_count = cast(int, os.cpu_count() if os.cpu_count() is not None else 1)
    # make sure each pool will have at least 50 files to move
    files_per_pool = 50
    # retry attempts on failed file moves
    retry_count = 3

    process_logger = ProcessLogger(
        "move_s3_objects",
        to_bucket=to_bucket,
        file_count=len(files),
    )
    process_logger.log_start()

    for retry_attempt in range(retry_count):
        max_pool_size = max(1, int(len(files_to_move) / files_per_pool))
        pool_size = min(32, cpu_count + 4, max_pool_size)
        process_logger.add_metadata(pool_size=pool_size)
        try:
            with Pool(pool_size, initializer=_init_process_session) as pool:
                for file_moved in pool.starmap(
                    _move_s3_object,
                    [(filename, to_bucket) for filename in files_to_move],
                ):
                    # remove filename from set if moved without exception
                    if isinstance(file_moved, str):
                        files_to_move.discard(file_moved)

        except Exception as exception:
            found_exception = exception

        # all files moved, exit retry loop
        if len(files_to_move) == 0:
            break

        # wait for gremlins to disappear
        time.sleep(15)

    process_logger.add_metadata(
        failed_count=len(files_to_move), retry_attempts=retry_attempt
    )

    if len(files_to_move) == 0:
        process_logger.log_complete()
    else:
        process_logger.log_failure(exception=found_exception)

    return list(files_to_move)


def write_parquet_file(
    table: Table,
    config_type: str,
    s3_path: str,
    partition_cols: List[str],
    visitor_func: Callable[..., None],
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
        "write_parquet", config_type=config_type, number_of_rows=table.num_rows
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
        file_visitor=visitor_func,
        basename_template=guid() + "-{i}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    process_logger.log_complete()
