import os
from multiprocessing import Pool, Queue
from typing import Dict, List

from lamp_py.aws.s3 import move_s3_objects

from .convert_gtfs import GtfsConverter
from .convert_gtfs_rt import GtfsRtConverter
from .converter import ConfigType, Converter
from .error import ConfigTypeFromFilenameException, NoImplException
from .utils import DEFAULT_S3_PREFIX, group_sort_file_list


class NoImplConverter(Converter):
    """
    Converter for incoming file formats that are unsupported. It passes all
    files from incoming through to the error bucket, triggering a move every 100
    files.
    """

    def convert(self) -> List[str]:
        move_s3_objects(
            self.files,
            os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
        )
        return self.files


def get_converter(config_type: ConfigType, metadata_queue: Queue) -> Converter:
    """
    get the correct converter for this config type. it may raise an exception if
    the gtfs_rt file type does not have an implemented detail
    """
    if config_type.is_gtfs():
        return GtfsConverter(config_type, metadata_queue)
    return GtfsRtConverter(config_type, metadata_queue)


def run_converter(converter: Converter) -> List[str]:
    """
    Run converters in subprocess
    """
    return converter.convert()


def ingest_files(files: List[str], metadata_queue: Queue) -> List[str]:
    """
    sort the incoming file list by type and create a converter for each type.
    each converter will ingest and convert its files in its own thread.
    """
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
                converters[config_type] = get_converter(
                    config_type, metadata_queue
                )
            converters[config_type].add_files(file_group)
        except (ConfigTypeFromFilenameException, NoImplException):
            error_files += file_group

    # GTFS Static Schedule must be processed first for performance manager to
    # work as expected
    if ConfigType.SCHEDULE in converters:
        processed_static_files = converters[ConfigType.SCHEDULE].convert()
        del converters[ConfigType.SCHEDULE]

    converters[ConfigType.ERROR] = NoImplConverter(
        ConfigType.ERROR, metadata_queue
    )
    converters[ConfigType.ERROR].add_files(error_files)

    # The remaining converters can be run in parallel
    #
    # the use of signal.signal, multiprocessing.Manager and multiprocessing.Pool.map, in combination,
    # causes inadvertent SIGTERM signals to be sent by application and main event loop to be blocked
    #
    # launching converter processes with map_async avoids throwing of SIGTERM signals and blocking
    with Pool(processes=len(converters)) as pool:
        result = pool.map_async(run_converter, converters.values())
        processed_realtime_files = result.get()

    # join all of the lists of processed files into a single list and return
    return sum(processed_realtime_files, processed_static_files)
