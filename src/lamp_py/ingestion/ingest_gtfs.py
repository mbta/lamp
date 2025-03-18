import os
from multiprocessing import get_context
from queue import Queue
from typing import (
    Dict,
    List,
    Optional,
)

from lamp_py.aws.s3 import (
    move_s3_objects,
    file_list_from_s3,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.convert_gtfs import GtfsConverter
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import (
    ConfigType,
    Converter,
)
from lamp_py.ingestion.error import (
    ConfigTypeFromFilenameException,
    NoImplException,
    IgnoreIngestion,
)
from lamp_py.runtime_utils.remote_files import LAMP, S3_ERROR, S3_INCOMING
from lamp_py.ingestion.utils import group_sort_file_list
from lamp_py.ingestion.compress_gtfs.gtfs_to_parquet import gtfs_to_parquet


class NoImplConverter(Converter):
    """
    Converter for incoming file formats that are unsupported. It passes all
    files from incoming through to the error bucket, triggering a move every 100
    files.
    """

    def convert(self) -> None:
        move_s3_objects(
            self.files,
            os.path.join(S3_ERROR, LAMP),
        )


def run_converter(converter: Converter) -> None:
    """
    Run converters in subprocess
    """
    converter.convert()


def ingest_gtfs_archive(metadata_queue: Queue[Optional[str]]) -> None:
    """
    ingest gtfs schedules from MBTA GTFS schedule archive
    """
    logger = ProcessLogger(process_name="ingest_gtfs")
    logger.log_start()

    gtfs_converter = GtfsConverter(ConfigType.SCHEDULE, metadata_queue)
    gtfs_converter.convert()

    logger.log_complete()


def ingest_s3_files(metadata_queue: Queue[Optional[str]]) -> None:
    """
    get all of the filepaths currently in the incoming bucket, sort them into
    batches of similar gtfs-rt files, convert each batch into tables, write the
    tables to parquet files in the springboard bucket, add the parquet
    filepaths to the metadata table as unprocessed, and move gtfs files to the
    archive bucket (or error bucket in the event of an error)
    """
    logger = ProcessLogger(process_name="ingest_s3_files")
    logger.log_start()

    try:
        files = file_list_from_s3(
            bucket_name=S3_INCOMING,
            file_prefix=LAMP,
        )

        grouped_files = group_sort_file_list(files)

        # initialize with an error / no impl converter, the rest will be added in as
        # the appear.
        converters: Dict[ConfigType, Converter] = {}
        error_files: List[str] = []

        for file_group in grouped_files.values():
            # get the config type from the file name and create a converter for this
            # type if one does not already exist. add the files to their converter.
            # if something goes wrong, add these files to the error converter where
            # they will be moved from incoming to error s3 buckets.
            try:
                config_type = ConfigType.from_filename(file_group[0])
                if config_type not in converters:
                    converters[config_type] = GtfsRtConverter(config_type, metadata_queue)
                converters[config_type].add_files(file_group)
            except IgnoreIngestion:
                continue
            except (ConfigTypeFromFilenameException, NoImplException):
                error_files += file_group

        converters[ConfigType.ERROR] = NoImplConverter(ConfigType.ERROR, metadata_queue)
        converters[ConfigType.ERROR].add_files(error_files)

    except Exception as exception:
        logger.log_failure(exception)

    # The remaining converters can be run in parallel
    #
    # Using signal.signal to detect ECS termination and multiprocessing.Manager
    # to manage the metadata queue along with multiprocessing.Pool.map causes
    # inadvertent SIGTERM signals to be sent and blocks the main event loop. To
    # fix this, we use multiprocessing.Pool.map_async. We use pool.close()
    # and pool.join() to ensure all work has completed in pools.
    #
    # Also worth noting, this application is run on Ubuntu when run on ECS,
    # who's default subprocess start method is "fork". On OSX, this default is
    # "spawn" some of the behavior described above only occurs when using
    # "fork". On OSX (and Windows?) to force this behavior, run
    # multiprocessing.set_start_method("fork") when starting the script.
    process_count = os.cpu_count()
    if process_count is None:
        process_count = 4
    if len(converters) > 0:
        with get_context("spawn").Pool(processes=process_count, maxtasksperchild=1) as pool:
            pool.map_async(run_converter, converters.values())
            pool.close()
            pool.join()

    logger.log_complete()


def ingest_gtfs(metadata_queue: Queue[Optional[str]]) -> None:
    """
    ingest all gtfs file types

    static schedule files should be ingested first
    """
    gtfs_to_parquet()
    ingest_gtfs_archive(metadata_queue)
    ingest_s3_files(metadata_queue)
