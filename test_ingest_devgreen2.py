import marimo

__generated_with = "0.10.12"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from multiprocessing import get_context
    from queue import Queue
    from typing import (
        Dict,
        List,
        Optional,
    )

    import pprint

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

    return (
        ConfigType,
        ConfigTypeFromFilenameException,
        Converter,
        Dict,
        GtfsConverter,
        GtfsRtConverter,
        IgnoreIngestion,
        LAMP,
        List,
        NoImplException,
        Optional,
        ProcessLogger,
        Queue,
        S3_ERROR,
        S3_INCOMING,
        file_list_from_s3,
        get_context,
        group_sort_file_list,
        gtfs_to_parquet,
        move_s3_objects,
        os,
        pprint,
    )


@app.cell
def _(file_list_from_s3):
    a = "s3://mbta-ctd-dataplatform-dev-incoming/lamp/delta/2025/02/02/2025-02-02T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"
    file = file_list_from_s3("mbta-ctd-dataplatform-dev-incoming", "lamp")
    return a, file


@app.cell
def _(file, group_sort_file_list, pprint):
    grouped_files = group_sort_file_list(file)
    # print(grouped_files)
    keys = grouped_files.keys()
    pprint.pprint(grouped_files.keys(), indent=2)
    print(len(keys))
    # grouped_files_devgreen = {key: grouped_files[key] for key in ["https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_VehiclePositions_enhanced.json.gz", "https_mbta_gtfs_s3_dev_green.s3.amazonaws.com_rtr_TripUpdates_enhanced.json.gz"]}

    # keys_devgreen = grouped_files_devgreen.keys()
    # print(keys_devgreen)
    # print(grouped_files_devgreen.values())
    return grouped_files, keys


@app.cell
def _():

    from lamp_py.aws.ecs import handle_ecs_sigterm, check_for_sigterm
    from lamp_py.aws.kinesis import KinesisReader
    from lamp_py.postgres.postgres_utils import start_rds_writer_process
    from lamp_py.runtime_utils.alembic_migration import alembic_upgrade_to_head
    from lamp_py.runtime_utils.env_validation import validate_environment

    # from lamp_py.runtime_utils.process_logger import ProcessLogger

    from lamp_py.ingestion.ingest_gtfs import ingest_gtfs
    from lamp_py.ingestion.glides import ingest_glides_events
    from lamp_py.ingestion.light_rail_gps import ingest_light_rail_gps

    return (
        KinesisReader,
        alembic_upgrade_to_head,
        check_for_sigterm,
        handle_ecs_sigterm,
        ingest_glides_events,
        ingest_gtfs,
        ingest_light_rail_gps,
        start_rds_writer_process,
        validate_environment,
    )


@app.cell
def _(start_rds_writer_process):
    metadata_queue, rds_process = start_rds_writer_process()

    return metadata_queue, rds_process


@app.cell
def _(validate_environment):
    from dotenv import load_dotenv

    # import os

    load_dotenv()

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
    return (load_dotenv,)


@app.cell
def _(
    ConfigType,
    ConfigTypeFromFilenameException,
    Converter,
    Dict,
    GtfsRtConverter,
    IgnoreIngestion,
    List,
    NoImplConverter,
    NoImplException,
    Optional,
    ProcessLogger,
    Queue,
    file_list_from_s3,
    get_context,
    group_sort_file_list,
    pprint,
    run_converter,
):

    def ingest_s3_files(metadata_queue: Queue[Optional[str]]) -> None:
        """
        get all of the filepaths currently in the incoming bucket, sort them into
        batches of similar gtfs-rt files, convert each batch into tables, write the
        tables to parquet files in the springboard bucket, add the parquet
        filepaths to the metadata table as unprocessed, and move gtfs files to the
        archive bucket (or error bucket in the event of an error)
        """
        logger = ProcessLogger(process_name="ingest_s3_files_hh")
        logger.log_start()

        try:
            # , "lamp")
            files = file_list_from_s3(
                bucket_name="mbta-ctd-dataplatform-dev-incoming",
                file_prefix="lamp",
            )

            grouped_files = group_sort_file_list(files)

            keys = grouped_files.keys()
            pprint.pprint(grouped_files.keys(), indent=2)
            print(len(keys))

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
        if len(converters) > 0:
            with get_context("spawn").Pool(processes=len(converters)) as pool:
                pool.map_async(run_converter, converters.values())
                pool.close()
                pool.join()

        logger.log_complete()

    return (ingest_s3_files,)


@app.cell
def _(ingest_s3_files, metadata_queue):
    ingest_s3_files(metadata_queue)
    return


@app.cell
def _(
    ConfigType,
    ConfigTypeFromFilenameException,
    Converter,
    Dict,
    IgnoreIngestion,
    List,
    NoImplException,
    grouped_files_devgreen,
):
    converters: Dict[ConfigType, Converter] = {}
    error_files: List[str] = []

    for file_group in grouped_files_devgreen.values():
        # get the config type from the file name and create a converter for this
        # type if one does not already exist. add the files to their converter.
        # if something goes wrong, add these files to the error converter where
        # they will be moved from incoming to error s3 buckets.
        print(file_group[0])
        try:
            config_type = ConfigType.from_filename(file_group[0])
            print(config_type)
            # if config_type not in converters:
            #     converters[config_type] = GtfsRtConverter(config_type, metadata_queue)
            # converters[config_type].add_files(file_group)
        except IgnoreIngestion:
            print("ignored")
        except (ConfigTypeFromFilenameException, NoImplException):
            print("failed")
            # error_files += file_group

        # converters[ConfigType.ERROR] = NoImplConverter(ConfigType.ERROR, metadata_queue)
        # converters[ConfigType.ERROR].add_files(error_files)
    return config_type, converters, error_files, file_group


if __name__ == "__main__":
    app.run()
