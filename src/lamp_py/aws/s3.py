import datetime
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import current_thread
from typing import (
    Callable,
    Dict,
    IO,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import boto3
import pandas
import pyarrow
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyarrow import Table, fs
from pyarrow.util import guid
from lamp_py.runtime_utils.process_logger import ProcessLogger


def get_s3_client() -> boto3.client:
    """Thin function needed for stubbing tests"""
    return boto3.client("s3")


def upload_file(
    file_name: str, object_path: str, extra_args: Optional[Dict] = None
) -> bool:
    """
    Upload a local file to an S3 Bucket

    :param file_name: local file path to upload
    :param object_path: S3 object path to upload to (including bucket)
    :param extra_agrs: additional upload ags available per: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS

    :return: True if file was uploaded, else False
    """
    upload_log = ProcessLogger(
        "s3_upload_file",
        file_name=file_name,
        object_path=object_path,
    )
    if isinstance(extra_args, dict):
        upload_log.add_metadata(**extra_args)
    upload_log.log_start()

    try:
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"{file_name} not found locally")

        object_path = object_path.replace("s3://", "")
        bucket, object_name = object_path.split("/", 1)

        s3_client = get_s3_client()

        s3_client.upload_file(
            file_name, bucket, object_name, ExtraArgs=extra_args
        )

        upload_log.log_complete()

        return True

    except Exception as exception:
        upload_log.log_failure(exception=exception)
        return False


def download_file(object_path: str, file_name: str) -> bool:
    """
    Download an S3 object to a local file
    will overwrite local file, if exists

    :param object_path: S3 object path to download from (including bucket)
    :param file_name: local file path to save object to

    :return: True if file was downloaded, else False
    """
    download_log = ProcessLogger(
        "s3_download_file",
        file_name=file_name,
        object_path=object_path,
    )
    download_log.log_start()

    try:
        if os.path.exists(file_name):
            os.remove(file_name)

        object_path = object_path.replace("s3://", "")
        bucket, object_name = object_path.split("/", 1)

        s3_client = get_s3_client()

        s3_client.download_file(bucket, object_name, file_name)

        download_log.log_complete()

        return True

    except Exception as exception:
        download_log.log_failure(exception=exception)
        return False


def delete_object(del_obj: str) -> bool:
    """
    delete s3 object

    :param del_obj - expected as 's3://my_bucket/object' or 'my_bucket/object'

    :return: True if file success, else False
    """
    try:
        process_logger = ProcessLogger("delete_s3_object", del_obj=del_obj)
        process_logger.log_start()

        s3_client = get_s3_client()

        # trim off leading s3://
        del_obj = del_obj.replace("s3://", "")

        # split into bucket and object name
        bucket, obj = del_obj.split("/", 1)

        # delete the source object
        _ = s3_client.delete_object(
            Bucket=bucket,
            Key=obj,
        )

        process_logger.log_complete()
        return True

    except Exception as error:
        process_logger.log_failure(error)
        return False


def object_metadata(obj: str) -> Dict[str, str]:
    """
    retrieve s3 object Metadata

    Will throw if object does not exist

    :param obj - expected as 's3://my_bucket/object' or 'my_bucket/object'

    :return: Metadata{"Key":"Value"}
    """
    try:
        process_logger = ProcessLogger("s3_object_metadata", obj=obj)
        process_logger.log_start()

        s3_client = get_s3_client()

        # trim off leading s3://
        obj = obj.replace("s3://", "")

        # split into bucket and object name
        bucket, obj = obj.split("/", 1)

        # delete the source object
        object_head = s3_client.head_object(
            Bucket=bucket,
            Key=obj,
        )

        process_logger.log_complete()
        return object_head["Metadata"]

    except Exception as error:
        process_logger.log_failure(error)
        raise error


def get_zip_buffer(filename: str) -> IO[bytes]:
    """
    Get a buffer for a zip file from s3 so that it can be read by zipfile
    module. filename is assumed to be the full path to the zip file without the
    s3:// prefix.
    """
    # inspired by
    # https://betterprogramming.pub/unzip-and-gzip-incoming-s3-files-with-aws-lambda-f7bccf0099c9
    (bucket, file) = filename.split("/", 1)
    s3_resource = boto3.resource("s3")
    zipped_file = s3_resource.Object(bucket_name=bucket, key=file)

    return BytesIO(zipped_file.get()["Body"].read())


def file_list_from_s3(
    bucket_name: str, file_prefix: str, max_list_size: int = 250_000
) -> List[str]:
    """
    get a list of s3 objects

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix filter for object keys

    :return List[
        object path as s3://bucket-name/object-key
    ]
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
                if obj["Size"] == 0:
                    continue
                filepaths.append(os.path.join("s3://", bucket_name, obj["Key"]))

            if len(filepaths) > max_list_size:
                break

        process_logger.add_metadata(list_size=len(filepaths))
        process_logger.log_complete()
        return filepaths
    except Exception as exception:
        process_logger.log_failure(exception)
        return []


def file_list_from_s3_with_details(
    bucket_name: str, file_prefix: str
) -> List[Dict]:
    """
    get a list of s3 objects with additional details

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix filter for object keys

    return_dict = {
        "s3_obj_path": "str: object path as s3://bucket-name/object-key",
        "size_bytes": "int: size of object in bytes",
        "last_modified": "datetime.datetime: object creation date",
    }

    :return List[return_dict]
    """
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_prefix)
    file_list = []

    for obj in response.get("Contents", []):
        if obj["Size"] == 0:
            continue

        file_list.append(
            {
                "s3_obj_path": os.path.join("s3://", bucket_name, obj["Key"]),
                "size_bytes": obj["Size"],
                "last_modified": obj["LastModified"],
            }
        )

    return file_list


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
        process_logger = ProcessLogger("move_s3_object", filename=filename)
        process_logger.log_start()

        s3_resource = current_thread().__dict__["boto_s3_resource"]

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
        response = source_object.delete()
        process_logger.add_metadata(**response)

    except Exception as error:
        _init_process_session()
        process_logger.log_failure(error)
        return None

    process_logger.log_complete()
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
    process_data = current_thread()
    process_data.__dict__["boto_session"] = boto3.session.Session()
    process_data.__dict__["boto_s3_resource"] = process_data.__dict__[
        "boto_session"
    ].resource("s3")


# pylint: disable=R0914
# pylint too many local variables (more than 15)
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
        results = []
        try:
            with ThreadPoolExecutor(
                max_workers=pool_size, initializer=_init_process_session
            ) as pool:
                for filename in files_to_move:
                    results.append(
                        pool.submit(_move_s3_object, filename, to_bucket)
                    )
            for result in results:
                current_result = result.result()
                if isinstance(current_result, str):
                    files_to_move.discard(current_result)

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


# pylint: enable=R0914


# pylint: disable=R0913
# pylint too many arguments (more than 5)
def write_parquet_file(
    table: Table,
    file_type: str,
    s3_dir: str,
    partition_cols: Optional[List[str]] = None,
    visitor_func: Optional[Callable[[str], None]] = None,
    basename_template: Optional[str] = None,
    filename: Optional[str] = None,
) -> None:
    """
    Helper function to write out a parquet table to an s3 path, partitioning
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

    @table - the table thats going to be written to parquet
    @file_type - string used in logging to indicate what type of file was
        written
    @s3_dir - the s3 bucket plus prefix "subdirectory" path where the
        parquet files should be written
    @partition_cols - column names in the table to partition out into the
        filepath. NOTE: the assumption is that the values in the partition
        columns of the incoming table are uniform.
    @visitor_func - if set, this function will be called with a WrittenFile
        instance for each file created during the call. a WrittenFile has
        path and metadata attributes.
    @basename_template - a template string used to generate base names of
        written parquet files. The token `{i}` will be replaced with an
        incremented int.
    @filename - if set, the filename that will be written to. if left empty,
        the basename template (or _its_ fallback) will be used.
    """
    process_logger = ProcessLogger(
        "write_parquet", file_type=file_type, number_of_rows=table.num_rows
    )
    process_logger.log_start()

    # pull out the partition information into a list of strings.
    if partition_cols is None:
        partition_cols = []

    partition_strings = []
    for col in partition_cols:
        unique_list = pc.unique(table.column(col)).to_pylist()

        assert (
            len(unique_list) == 1
        ), f"Table {s3_dir} column {col} had {len(unique_list)} unique elements"

        partition_strings.append(f"{col}={unique_list[0]}")

    table = table.drop(partition_cols)

    # generate an s3 path to write this file to. if there is a filename, use
    # that. if not use the basename template, with its uuid fallback, to
    # generate the filename
    if filename is None:
        if basename_template is None:
            basename_template = guid() + "-{i}.parquet"
        filename = basename_template.format(i=0)

    write_path = os.path.join(s3_dir, *partition_strings, filename)

    process_logger.add_metadata(write_path=write_path)

    # write teh parquet file to the partitioned path
    with pq.ParquetWriter(
        where=write_path, schema=table.schema, filesystem=fs.S3FileSystem()
    ) as pq_writer:
        pq_writer.write(table)

    # call the visitor function if it exists
    if visitor_func is not None:
        visitor_func(write_path)

    process_logger.log_complete()


# pylint: enable=R0913


def get_datetime_from_partition_path(path: str) -> datetime.datetime:
    """
    process and return datetime from partitioned s3 path
    """
    try:
        # handle gtfs-rt paths
        year = int(re.findall(r"year=(\d{4})", path)[0])
        month = int(re.findall(r"month=(\d{1,2})", path)[0])
        day = int(re.findall(r"day=(\d{1,2})", path)[0])
        hour = int(re.findall(r"hour=(\d{1,2})", path)[0])
        return_date = datetime.datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            tzinfo=datetime.timezone.utc,
        )
    except IndexError as _:
        # handle gtfs static paths
        timestamp = float(re.findall(r"timestamp=(\d{10})", path)[0])
        return_date = datetime.datetime.fromtimestamp(timestamp)
    return return_date


def _get_pyarrow_table(
    filename: Union[str, List[str]],
    filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
) -> pyarrow.Table:
    """
    internal function to get pyarrow table from parquet file(s)
    """

    active_fs = fs.S3FileSystem()

    if isinstance(filename, list):
        to_load = [f.replace("s3://", "") for f in filename]
    else:
        to_load = [filename.replace("s3://", "")]

    # using `read_pandas` because `read` with "columns" parameter results in
    # much slower file downloads for some reason...
    return pq.ParquetDataset(
        to_load, filesystem=active_fs, filters=filters
    ).read_pandas()


def read_parquet(
    filename: Union[str, List[str]],
    columns: Union[List[str], slice] = slice(None),
    filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
) -> pandas.core.frame.DataFrame:
    """
    read parquet file or files from s3 and return it as a pandas dataframe
    """
    return (
        _get_pyarrow_table(filename, filters)
        .to_pandas(self_destruct=True)
        .loc[:, columns]
    )


def read_parquet_chunks(
    filename: Union[str, List[str]],
    max_rows: int = 100_000,
    columns: Union[List[str], slice] = slice(None),
    filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
) -> Iterator[pandas.core.frame.DataFrame]:
    """
    read parquet file or files from s3 IN CHUNKS
    return chunks as pandas dataframes

    chunk size attempts to be close to max_rows parameter, but may sometimes
    overshoot because of chunk layout of pyarrow table
    """
    yield_dataframe = pandas.DataFrame()
    for batch in _get_pyarrow_table(filename, filters).to_batches(
        max_chunksize=None
    ):
        yield_dataframe = pandas.concat(
            [yield_dataframe, batch.to_pandas().loc[:, columns]]
        )
        if yield_dataframe.shape[0] >= max_rows:
            yield yield_dataframe
            yield_dataframe = pandas.DataFrame()

    if yield_dataframe.shape[0] > 0:
        yield yield_dataframe
