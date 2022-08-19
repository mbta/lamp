import logging
import os
from collections.abc import Iterable
from typing import Optional, List, Tuple, Union, Sequence

import boto3
import pandas
import pyarrow.parquet as pq
from pyarrow import fs


def read_parquet(
    filename: Union[str, List[str]],
    columns: Union[List[str], slice] = slice(None),
    filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
) -> pandas.core.frame.DataFrame:
    """
    read parquet file or files from s3 and return it as a pandas dataframe
    """
    active_fs = fs.S3FileSystem()
    if isinstance(filename, list):
        to_load = [f.replace("s3://", "") for f in filename]
    else:
        to_load = [filename.replace("s3://", "")]

    return (
        pq.ParquetDataset(to_load, filesystem=active_fs, filters=filters)
        .read_pandas()
        .to_pandas()
        .loc[:, columns]
    )


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
            # skip if this object is a "directory"
            if obj["Size"] == 0:
                continue
            uri = os.path.join("s3://", bucket_name, obj["Key"])
            logging.debug(uri)
            yield uri
