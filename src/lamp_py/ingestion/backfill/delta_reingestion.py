# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from concurrent.futures import ThreadPoolExecutor
import copy
import logging
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from queue import Queue
from sys import prefix
import tempfile
from typing import Dict, Iterable, List, Optional, Tuple
import pyarrow
from pyarrow import fs
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import dataframely as dy
import polars as pl

from lamp_py.aws.ecs import running_in_aws
from lamp_py.ingestion.backfill.convert_gtfs_rt_fullset import GtfsRtTripsFullSetConverter
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.aws.s3 import file_list_from_s3, upload_file
from lamp_py.runtime_utils.remote_files import S3_INCOMING, springboard_rt_vehicle_positions
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3Location

from lamp_py.tableau.conversions import convert_gtfs_rt_vehicle_position
from lamp_py.tableau.conversions.convert_gtfs_rt_trip_updates import filter_valid_devgreen_terminal_predictions
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.lamp_jobs import GTFS_RT_TABLEAU_PROJECT
from lamp_py.utils.filter_bank import FilterBankRtVehiclePositions, HeavyRailFilter, LightRailFilter
import json
import pyarrow.compute as pc


def write_dataset_to_single_parquet_partitioned_and_sorted(
    local_path: str,
    output_parquet_path: str,
    partition_column: str,
    in_partition_sort: List[Tuple[str, str]],
    debug_flag: bool = False,
) -> None:

    logger = ProcessLogger("write_dataset_to_single_parquet_partitioned_and_sorted")
    ds_paths = os.listdir(local_path)
    ds_paths = [os.path.join(local_path, s) for s in ds_paths]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=fs.LocalFileSystem(),
    )

    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        # include the hash column for debug
        writer = pq.ParquetWriter(output_parquet_path, schema=ds.schema, compression="zstd", compression_level=3)

        partitions = pc.unique(ds.to_table(columns=[partition_column]).column(partition_column))

        # grab just the partition values and sort them
        # this will be big, but not too big
        partitions = sorted(partitions.to_pylist())

        logger.add_metadata("unique_partitions", len(partitions))

        if debug_flag:
            start_time = time.time()

        for part in partitions:
            write_table = ds.to_table(filter=((pc.field(partition_column) == part))).sort_by(in_partition_sort)

            writer.write_table(write_table)

            if debug_flag:
                elapsed = time.time() - start_time
                logger.add_metadata(f"Processed partition {part}", f"{elapsed:.2f}s elapsed, total rows: {write_table.num_rows}")

        writer.close()


def delta_reingestion_runner(
    start_date: date,
    end_date: date,
    final_output_path: S3Location,
    converter_template_instance: GtfsRtConverter,
    in_filter: str | None = None,
    local_output_location: str = "/tmp/gtfs-rt-continuous/",
    bucket: str = S3_ARCHIVE | S3_INCOMING,
) -> None:
    """
    Docstring for delta_reingestion_runner

    :param start_date: start of reingestion range
    :type start_date: date
    :param end_date: end of reingestion range
    :type end_date: date
    :param final_output_base: final resting prefix-path of output artifacts
    :type final_output_base: S3Location
    :param local_output_location: temporary working directory to place outputs
    :type local_output_location: str

    """
    logger = ProcessLogger("delta_reingestion_runner")
    logger.log_start()

    cur_date = start_date

    while cur_date <= end_date:
        prefix = (
            os.path.join(
                LAMP,
                "delta",
                cur_date.strftime("%Y"),
                cur_date.strftime("%m"),
                cur_date.strftime("%d"),
            )
            + "/"
        )
        file_list = file_list_from_s3(
            bucket,
            prefix,
            in_filter=in_filter,
        )

        logger.add_metadata("file_list_length", len(file_list))

        #### Stage 1: local to local (MANY to many)
        converter = copy.deepcopy(converter_template_instance)
        converter.add_files(file_list)
        converter.convert()

        ## Stage 2: local to local (many to 1)
        converter
        # Define the path to your input Parquet files (can use a glob pattern)
        converter_output_path = f"{local_output_location}/lamp/{str(converter.config_type)}/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/"
        consolidated_parquet_output_file = (
            f"{local_output_location}/{cur_date.year}_{cur_date.month}_{cur_date.day}.parquet"
        )

        write_dataset_to_single_parquet_partitioned_and_sorted(
            converter_output_path,
            consolidated_parquet_output_file,
            partition_column=converter.partition_column(),
            in_partition_sort=converter.table_sort_order(),
            debug_flag=True,
        )

        #### Stage 3: local to remote (one to one)

        # upload local to remote
        upload_file(
            consolidated_parquet_output_file,
            consolidated_parquet_output_file.replace(
                f"{local_output_location}/",
                f"{final_output_path.s3_uri}/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/",
            ),
        )

        cur_date = cur_date + timedelta(days=1)

        converter.clean_local_folders()  # clear local output folders after each day to manage disk space
