#!/usr/bin/env python

import gzip
import os
from queue import Queue
import time
import logging
import signal
# import json
import pyarrow.json
# import gzip
from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.aws.s3 import *
from lamp_py.ingestion.converter import ConfigType
from lamp_py.ingestion.utils import group_sort_file_list
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.ingest_gtfs import ingest_gtfs, ingest_s3_files
from lamp_py.ingestion.glides import ingest_glides_events
from lamp_py.ingestion.light_rail_gps import ingest_light_rail_gps
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.runtime_utils.remote_files import LAMP, S3_ERROR, S3_INCOMING
import pyarrow
from pyarrow import fs

from dotenv import load_dotenv
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
    # while True:
    process_logger = ProcessLogger(process_name="run_ingestion_gtfs_rt_hhh")
    process_logger.log_start()

    # check_for_sigterm(metadata_queue, rds_process)
    ingest_s3_files(metadata_queue, config)
    # check_for_sigterm(metadata_queue, rds_process)

    # process_logger.log_complete()

        # time.sleep(30)

    process_logger.log_complete()
    metadata_queue.put(None)
    rds_process.join()
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


if __name__ == "__main__":

    # hhuang@TID27670HHUANG test2 % cksum *
    # 3360086869 8083 dev.json.gz
    # 2311245126 110782 dev_console.json.gz
    # 3360086869 8083 prod.json.gz
    # 2311245126 110782 prod_console.json.gz
    # # with open('/Users/hhuang/lamp/data/2025-02-20T00_00_00Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz', 'r') as fd:
    #     gzip_fd = gzip.GzipFile(fileobj=fd)
    #     data = json.load(gzip_fd)
    # with open('/Users/hhuang/lamp/data/2025-02-20T00_00_00Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json', 'r') as file:
    #     data = json.load(file)
  
    # with open('/Users/hhuang/lamp/test/dev.json.gz', 'r') as file:
    #         # with open('/Users/hhuang/lamp/dev_incoming_data/2025-02-21T02:51:26Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json.gz', 'r') as file:
    #     data2 = json.load(file)
    # with open('/Users/hhuang/lamp/test/prod.json.gz', 'r') as file:
    #         # with open('/Users/hhuang/lamp/dev_incoming_data/2025-02-21T02:51:26Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json.gz', 'r') as file:
    #     data2 = json.load(file)

    # with open('/Users/hhuang/lamp/prod_incoming_data/2025-02-21T02_51_26Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json.gz', 'r') as file:
    #     data2 = json.load(file)
    # print(data2)

    # with open('/Users/hhuang/lamp/dev_incoming_data/wtf.json.gz', 'r') as file:
    #         # with open('/Users/hhuang/lamp/dev_incoming_data/2025-02-21T02:51:26Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json.gz', 'r') as file:

    #     data2 = json.load(file)
    # print(data2)

    # with open('/Users/hhuang/Downloads/2025-02-19T00:02:55Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json', 'r') as file:
    #     data = json.load(file)
    # print(data)
    # thread_data = current_thread()
    # if self.files and self.files[0].startswith("s3://"):
    #     thread_data.__dict__["file_system"] = fs.S3FileSystem()
    # else:
    #     thread_data.__dict__["file_system"] = fs.LocalFileSystem()

    # # this works!
    # s3 = fs.S3FileSystem(region='us-east-1')
    # s3_uri = 's3://mbta-ctd-dataplatform-dev-incoming/lamp/delta/2025/02/20/2025-02-20T00:00:00Z_https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_TripUpdates_enhanced.json.gz'
    # # filename = 'mbta-ctd-dataplatform-dev-incoming/lamp/delta/2025/02/20/2025-02-20T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz'
    # filename = s3_uri.replace('s3://', '')
    # info = s3.get_file_info(filename)
    # # breakpoint()
    # print(info)
    # # filename2 = 'mbta-ctd-dataplatform-dev-incoming/lamp/delta/2025/02/20/2025-02-20T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz'

    # # exit()
    # with s3.open_input_stream(filename) as file:
    #     json_data = json.load(file)

    # print(json_data)

    # # this works!
    # exit()
    prefix = "lamp/delta/2025/02/20/"
    # in_filter = "https_cdn.mbta.com_realtime_TripUpdates"
    in_filter = "dev_green.s3.amazonaws.com_rtr_VehiclePositions"
    load_dotenv()
    CONFIG_FILE_LIST_FROM_S3 = {
        "bucket_name": S3_INCOMING,
        "file_prefix": prefix,
        "max_list_size": 10,
        "in_filter": in_filter,
        "multiprocessing": True,
    }

    start(CONFIG_FILE_LIST_FROM_S3)

    # mbta-ctd-dataplatform-dev-incoming
    # input args to start a specific runner
    # start(s3_path='s3://mbta-ctd-dataplatform-dev-incoming/', prefix='lamp/delta/2025/02/19/')
    # files = file_list_from_s3(bucket_name=S3_INCOMING, file_prefix=prefix, )
    # files = ingest_s3_files(metadata_queue, bucket_name=S3_INCOMING, file_prefix=prefix, max_list_size=50, in_filter="dev_green")

    # print(files)

    # grouped_files = group_sort_file_list(files)
