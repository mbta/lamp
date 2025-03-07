#!/usr/bin/env python

from datetime import datetime, timezone
import json
import os
import tempfile
from typing import Optional, Tuple
import pyarrow
from pyarrow import fs

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import polars as pl


from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import GTFS_RT_HASH_COL, hash_gtfs_rt_parquet, hash_gtfs_rt_table
from lamp_py.postgres.postgres_utils import start_rds_writer_process

def gz_to_pyarrow_local(filename: str, schema: pyarrow.schema) -> Tuple[Optional[datetime], str, Optional[pyarrow.table]]:
    """
    Convert a gzipped json of gtfs realtime data into a pyarrow table. This
    function is executed inside of a thread, so all exceptions must be
    handled internally.

    @filename file of gtfs rt data to be converted (file system chosen by
        GtfsRtConverter in thread_init)

    @return Optional[datetime] - datetime contained in header of gtfs rt
        feed. (returns None if an Exception is thrown during conversion)
    @return str - input filename with s3 prefix stripped out.
    @return Optional[pyarrow.table] - the pyarrow table of gtfs rt data that
        has been converted. (returns None if an Exception is thrown during
        conversion)
    """
    # breakpoint()
    file_system = fs.LocalFileSystem()    
    try:
        with file_system.open_input_stream(filename) as file:
            json_data = json.load(file)

    except UnicodeDecodeError as _:
        with file_system.open_input_stream(filename, compression="gzip") as file:
            json_data = json.load(file)

    # parse timestamp info out of the header
    feed_timestamp = json_data["header"]["timestamp"]
    timestamp = datetime.fromtimestamp(feed_timestamp, timezone.utc)

    table = pyarrow.Table.from_pylist(json_data["entity"], schema=schema)

    table = table.append_column(
        "year",
        pyarrow.array([timestamp.year] * table.num_rows, pyarrow.uint16()),
    )
    table = table.append_column(
        "month",
        pyarrow.array([timestamp.month] * table.num_rows, pyarrow.uint8()),
    )
    table = table.append_column(
        "day",
        pyarrow.array([timestamp.day] * table.num_rows, pyarrow.uint8()),
    )
    table = table.append_column(
        "feed_timestamp",
        pyarrow.array([feed_timestamp] * table.num_rows, pyarrow.uint64()),
    )

    return (
        timestamp,
        filename,
        table,
    )

def make_hash_dataset(table: pyarrow.Table, local_path: str) -> pyarrow.dataset:
    """
    create dataset, with hash column, that will be written to parquet file

    :param table: pyarrow Table
    :param local_path: path to local parquet file
    """
    table = hash_gtfs_rt_table(table)
    out_ds = pd.dataset(table)

    if os.path.exists(local_path):
        hash_gtfs_rt_parquet(local_path)
            # RT_ALERTS parquet files contain columns with nested structure types
            # if a new nested field is ingested, combining of the new and existing nested column is not possible
            # this try/except is meant to catch that error and reset the schema for the sevice day to the new nested structure
            # RT_ALERTS updates are essentially the same throughout a service day so resetting the
            # dataset will have minimal impact on archived data
            # try:
        out_ds = pd.dataset(
            [
                pd.dataset(table),
                pd.dataset(local_path),
            ]
        )
            # except pyarrow.ArrowTypeError as exception:
            #     if self.config_type == ConfigType.RT_ALERTS:
            #         out_ds = pd.dataset(table)
            #     else:
            #         raise exception

    return out_ds

def write_local_pq_v2(table: pyarrow.Table, local_path: str, config: GtfsRtConverter) -> None:
    """
    merge pyarrow Table with existing local_path parquet file

    :param table: pyarrow Table
    :param local_path: path to local parquet file
    """

    # breakpoint()
    out_ds = make_hash_dataset(table, local_path)

    unique_ts_min = pc.min(table.column("feed_timestamp")).as_py() - (60 * 45)

    no_hash_schema = out_ds.schema.remove(out_ds.schema.get_field_index(GTFS_RT_HASH_COL))

    with tempfile.TemporaryDirectory() as temp_dir:
        hash_pq_path = os.path.join(temp_dir, "hash.parquet")
        upload_path = os.path.join(temp_dir, "upload.parquet")
        hash_writer = pq.ParquetWriter(hash_pq_path, schema=out_ds.schema)
        upload_writer = pq.ParquetWriter(upload_path, schema=no_hash_schema)

        breakpoint()
        partitions = pc.unique(
            out_ds.to_table(columns=[config.detail.partition_column]).column(config.detail.partition_column)
        )
        for part in partitions:
            unique_table = (
                pl.DataFrame(
                    out_ds.to_table(
                        filter=(
                            (pc.field(config.detail.partition_column) == part)
                            & (pc.field("feed_timestamp") >= unique_ts_min)
                        )
                    )
                )
                .sort(by=["feed_timestamp"])
                .unique(subset=GTFS_RT_HASH_COL, keep="first")
                .to_arrow()
                .cast(out_ds.schema)
            )
            ds_table = out_ds.to_table(
                filter=(
                    (pc.field(config.detail.partition_column) == part) & (pc.field("feed_timestamp") < unique_ts_min)
                )
            )
            # write these two separately
            write_table = pyarrow.concat_tables([unique_table, ds_table])
            hash_writer.write_table(write_table)

            # drop GTFS_RT_HASH_COL column for S3 upload
            upload_writer.write_table(write_table.drop_columns(GTFS_RT_HASH_COL))

        hash_writer.close()
        # upload_writer.close()
        os.replace(hash_pq_path, local_path)
        # os.replace(upload_writer, local_path + "hash")

        # upload_file(
        #     upload_path,
        #     local_path.replace(self.tmp_folder, S3_SPRINGBOARD),
        # )

def test_():
    local_file_path = '/Users/hhuang/lamp/data_py/2025-02-20T00:00:32Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz'
    # config_type = ConfigType()
    metadata_queue, rds_process = start_rds_writer_process()
    converter = GtfsRtConverter(config_type=ConfigType.RT_TRIP_UPDATES, metadata_queue=metadata_queue)
    # table = pyarrow.Table()

    (date_str,table,rt_data)= gz_to_pyarrow_local(filename=local_file_path, schema=converter.detail.import_schema)
    local_path = '/Users/hhuang/lamp/lamp/investigation/hash.parquet'
    # breakpoint()
    # converter.write_local_pq_v2(table, local_path)
    table = converter.detail.transform_for_write(rt_data)
    for i in range(0,100):
        # trip_update.trip.route_id
        
        rt_tmp = table['trip_update.trip.route_id'].apply(lambda x: x + i)
        pyarrow.concat_tables([table, rt_tmp])
    

    breakpoint()
    write_local_pq_v2(table, local_path, converter)

