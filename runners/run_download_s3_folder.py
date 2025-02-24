#!/usr/bin/env python

import os
from queue import Queue
import time
import logging
import signal
import json
import argparse
import gzip

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.aws.s3 import *
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import group_sort_file_list
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.ingest_gtfs import ingest_gtfs, ingest_s3_files
from lamp_py.ingestion.glides import ingest_glides_events
from lamp_py.ingestion.light_rail_gps import ingest_light_rail_gps
from lamp_py.runtime_utils.remote_files import LAMP, S3_ERROR, S3_INCOMING
import pyarrow
from pyarrow import fs, json

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point For GTFS Ingestion Scripts"""


def main(config: Dict) -> None:
    """
    run the ingestion pipeline

    * setup metadata queue metadata writer process
    * setup a glides kinesis reader
    * on a loop
        * check to see if the pipeline should be terminated
        * ingest files from incoming s3 bucket
        * ingest glides events from kinesis
    """
    # start rds writer process
    # this will create only one rds engine while app is running
    metadata_queue, rds_process = start_rds_writer_process()

    # # connect to the glides kinesis stream
    # glides_reader = KinesisReader(stream_name="ctd-glides-prod")

    # run the event loop every 30 seconds
    while True:
        process_logger = ProcessLogger(process_name="run_ingestion_gtfs_rt_hhh")
        process_logger.log_start()

        check_for_sigterm(metadata_queue, rds_process)
        ingest_s3_files(metadata_queue, config)
        check_for_sigterm(metadata_queue, rds_process)

        process_logger.log_complete()

        time.sleep(30)

    process_logger.log_complete()

    # time.sleep(30)


def start(config: Dict) -> None:
    """configure and start the ingestion process"""
    # setup handling shutdown commands
    signal.signal(signal.SIGTERM, handle_ecs_sigterm)

    # configure the environment
    os.environ["SERVICE_NAME"] = "ingestion"

    validate_environment(
        required_variables=[
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
            "INCOMING_BUCKET",
            "PUBLIC_ARCHIVE_BUCKET",
            "SPRINGBOARD_BUCKET",
            "ALEMBIC_MD_DB_NAME",
        ],
        db_prefixes=["MD", "RPM"],
    )

    # run metadata rds migrations
    alembic_upgrade_to_head(db_name=os.environ["ALEMBIC_MD_DB_NAME"])

    # run the main method
    main(config)

def download_s3(file):
    # for file in files:
    path = file.replace("s3://", "")

    (s3_bucket, s3_path) = path.split(prefix)
    s3_bucket = s3_bucket.replace("/", "")
    # (drive, path) = os.path.splitdrive(files[0])
    # download_file(files[0], '/Users/hhuang/lamp/data_py/' + file)
    local_file_path = '/Users/hhuang/lamp/data_py/' + s3_path
    s3_object_path = prefix + s3_path

    s3.download_file(s3_bucket, s3_object_path, local_file_path) 

    print(f"File downloaded from S3 to: {local_file_path}") 

if __name__ == "__main__":

    prefix = "lamp/delta/2025/02/20/"
    in_filter = "realtime_VehiclePositions_enhance"
    config = {
        "bucket_name": S3_INCOMING,
        "file_prefix": prefix,
        "max_list_size": 100,
        "in_filter": in_filter,
        "multiprocessing": True,
    }
    files = file_list_from_s3(
            bucket_name=config["bucket_name"],
            file_prefix=config["file_prefix"],
            # max_list_size=config["max_list_size"],
            # in_filter=config["in_filter"],
        )


    breakpoint()
    s3 = boto3.client('s3')  

    with ThreadPoolExecutor(max_workers=10) as pool:
        pool.map(download_s3, files)

    # for file in files:
    #     path = file.replace("s3://", "")

    #     (s3_bucket, s3_path) = path.split(prefix)
    #     s3_bucket = s3_bucket.replace("/", "")
    #     # (drive, path) = os.path.splitdrive(files[0])
    #     # download_file(files[0], '/Users/hhuang/lamp/data_py/' + file)
    #     local_file_path = '/Users/hhuang/lamp/data_py/' + s3_path
    #     s3_object_path = prefix + s3_path

    #     s3.download_file(s3_bucket, s3_object_path, local_file_path) 

    #     print(f"File downloaded from S3 to: {local_file_path}") 


    # mdq = Queue
    # # # filename='/Users/hhuang/lamp/data/2025-02-20T00_00_00Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz'
    # gg = GtfsRtConverter(metadata_queue=mdq, config_type=ConfigType.RT_VEHICLE_POSITIONS)
    # data = gg.gz_to_pyarrow(filename=local_file_path)
    # print(data)

    def read_compressed_json_to_pyarrow(file_path):
        """
        Reads a gzip-compressed JSON file into a pyarrow table.

        Args:
            file_path (str): The path to the json.gz file.

        Returns:
            pyarrow.Table: A pyarrow table representing the data in the JSON file.
        """
        try:
            with gzip.open(file_path, 'r') as f:
                table = pyarrow.json.read_json(f)
                return table
        except FileNotFoundError:
            print(f"Error: File not found at '{file_path}'")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    # table = read_compressed_json_to_pyarrow(local_file_path)
    # breakpoint()

    # # # Example usage:
    # # file_path = 'your_data.json.gz'
    # # table = read_compressed_json_to_pyarrow(file_path)

    # if table is not None:
    #     print(table)

    exit()