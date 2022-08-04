import pyarrow.parquet as pq
from pyarrow import fs
from collections.abc import Iterable
import boto3
import logging
import os


def read_parquet(filename, filters=None):
    active_fs = fs.S3FileSystem()
    file_to_load = str(filename).replace("s3://", "")

    pandas_dataframe = (
        pq.ParquetDataset(file_to_load, filesystem=active_fs, filters=filters)
        .read_pandas()
        .to_pandas()
    )

    return pandas_dataframe


def file_list_from_s3(bucket_name: str, file_prefix: str) -> Iterable[str]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :yield filename, filesize tuples from inside of the bucket
    """
    logging.info(
        "Getting files with prefix %s from %s", file_prefix, bucket_name
    )
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=file_prefix)
    for page in pages:
        if page["KeyCount"] == 0:
            continue
        for obj in page["Contents"]:
            uri = os.path.join("s3://", bucket_name, obj["Key"])
            logging.debug(uri)
            yield uri
