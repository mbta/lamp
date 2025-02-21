#!/usr/bin/env python

import os
import time
import logging
import signal
from typing import Dict

from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.ingest_gtfs import ingest_gtfs
from lamp_py.ingestion.glides import ingest_glides_events
from lamp_py.ingestion.light_rail_gps import ingest_light_rail_gps
from lamp_py.runtime_utils.remote_files import LAMP, S3_INCOMING

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

    # connect to the glides kinesis stream
    glides_reader = KinesisReader(stream_name="ctd-glides-prod")

    # run the event loop every 30 seconds
    while True:
        process_logger = ProcessLogger(process_name="main")
        process_logger.log_start()

        check_for_sigterm(metadata_queue, rds_process)
        ingest_light_rail_gps()
        ingest_gtfs(metadata_queue, config)
        ingest_glides_events(glides_reader, metadata_queue)
        check_for_sigterm(metadata_queue, rds_process)

        process_logger.log_complete()

        time.sleep(30)


def start( config: Dict ) -> None:
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
    CONFIG_FILE_LIST_FROM_S3 = {"bucket_name" : S3_INCOMING, 
                            "file_prefix" : LAMP, 
                            "max_list_size" : 250_000, 
                            "in_filter": None,
                            "multiprocessing": True
                            }
    start(CONFIG_FILE_LIST_FROM_S3)
