import logging
import os

from concurrent.futures import ThreadPoolExecutor
from typing import List

from .converter import ConfigType, Converter
from .convert_gtfs import GtfsConverter
from .convert_gtfs_rt import GtfsRtConverter
from .error import ConfigTypeFromFilenameException, NoImplException
from .s3_utils import move_s3_objects
from .utils import DEFAULT_S3_PREFIX, group_sort_file_list


class NoImplConverter(Converter):
    """
    Converter for incoming file formats that are unsupported. It passes all
    files from incoming through to the error bucket, triggering a move every 100
    files.
    """

    def convert(self) -> None:
        move_s3_objects(
            self.files,
            os.path.join(os.environ["ERROR_BUCKET"], DEFAULT_S3_PREFIX),
        )


def get_converter(config_type: ConfigType, files: List[str]) -> Converter:
    """
    Get the correct converter object for a given config type
    """
    try:
        if config_type.is_gtfs():
            return GtfsConverter(config_type, files)
        if config_type.is_gtfs_rt():
            return GtfsRtConverter(config_type, files)
    except NoImplException as exception:
        # if we encounter a no impl exception, use a no impl converter that will
        # push all files to the error list
        logging.warning(exception)

    return NoImplConverter(config_type, files)


def ingest_files(files: List[str]) -> None:
    """
    sort the incoming file list by type and create a converter for each type.
    each converter will ingest and convert its files in its own thread.
    """
    grouped_files = group_sort_file_list(files)

    converters = []
    for file_group in grouped_files.values():
        logging.info(file_group)
        try:
            config_type = ConfigType.from_filename(file_group[0])
            converters.append(get_converter(config_type, file_group))
        except ConfigTypeFromFilenameException:
            converters.append(NoImplConverter(ConfigType.ERROR, file_group))

    with ThreadPoolExecutor(max_workers=len(converters)) as executor:
        for converter in converters:
            executor.submit(converter.convert)
