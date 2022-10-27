import logging

from collections.abc import Iterable
from typing import List, Tuple

import pyarrow

from .converter import ConfigType, Converter
from .convert_gtfs import GtfsConverter
from .convert_gtfs_rt import GtfsRtConverter
from .error import ConfigTypeFromFilenameException, NoImplException


class NoImplConverter(Converter):
    """
    Converter for incoming file formats that are unsupported. It passes all
    files from incoming through to the error bucket, triggering a move every 100
    files.
    """

    def get_tables(self) -> List[Tuple[str, pyarrow.Table]]:
        return []

    def add_file(self, file: str) -> bool:
        self.error_files.append(file)
        return len(self.error_files) > 100

    def reset(self) -> None:
        self.error_files = []

    @property
    def partition_cols(self) -> list[str]:
        return []


def get_converter(config_type: ConfigType) -> Converter:
    """
    Get the correct converter object for a given config type
    """
    try:
        if config_type.is_gtfs():
            return GtfsConverter(config_type)
        if config_type.is_gtfs_rt():
            return GtfsRtConverter(config_type)
    except NoImplException as exception:
        # if we encounter a no impl exception, use a no impl converter that will
        # push all files to the error list
        logging.warning(exception)

    return NoImplConverter(config_type)


def ingest_files(files: Iterable[str]) -> Iterable[Converter]:
    """
    Take a bunch of files and sort them into Batches based on their config type
    (derrived from filename). Each Batch should be under a limit in total
    filesize.

    :param file: An iterable of filename and filesize tubles to be sorted into
        Batches. The filename is used to determine config type.
    :param threshold: upper bounds on how large the sum filesize of a batch can
        be.

    :yield: Converter object containing a list of tables to write with their
            prefixes and files to move to error and archive buckets
    """
    converters = {t: get_converter(t) for t in ConfigType}

    for file in files:
        try:
            config_type = ConfigType.from_filename(file)
            converter = converters[config_type]
            save_file = converter.add_file(file)

            if save_file:
                yield converter
                converter.reset()

        except ConfigTypeFromFilenameException as config_exception:
            logging.warning(config_exception)
            converters[ConfigType.ERROR].add_file(file)
            continue

    for _, converter in converters.items():
        yield converter
