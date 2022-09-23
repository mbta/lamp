import os
from typing import Optional, List, Tuple, Union, Sequence, Iterator

import boto3
import pandas
import pyarrow.parquet as pq
from pyarrow import fs
import pyarrow


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


def file_list_from_s3(bucket_name: str, file_prefix: str) -> Iterator[str]:
    """
    generate filename, filesize tuples for every file in an s3 bucket

    :param bucket_name: the name of the bucket to look inside of
    :param file_prefix: prefix for files to generate

    :yield filename, filesize tuples from inside of the bucket
    """
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
            yield uri
