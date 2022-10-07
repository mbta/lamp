import logging

from collections.abc import Iterable
from typing import List

from .converter import ConfigType
from .error import ConfigTypeFromFilenameException
from .logging_utils import ProcessLogger


class Batch:
    """
    will store a collection of filenames that should be downloaded and converted
    from json into parquet.
    """

    def __init__(self, config_type: ConfigType) -> None:
        self.config_type = config_type
        self.total_size = 0

        self.prefix = ""
        self.suffix = ""
        self.filenames: List[str] = []

    def __str__(self) -> str:
        return (
            f"Batch of {self.total_size} bytes in {len(self.filenames)} "
            f"{self.config_type} files"
        )

    def add_file(self, filename: str, filesize: int) -> None:
        """Add a file to this batch"""
        self.filenames.append(filename)
        self.total_size += filesize


def batch_files(
    files: Iterable[tuple[str, int]], threshold: int
) -> Iterable[Batch]:
    """
    Take a bunch of files and sort them into Batches based on their config type
    (derrived from filename). Each Batch should be under a limit in total
    filesize.

    :param file: An iterable of filename and filesize tubles to be sorted into
        Batches. The filename is used to determine config type.
    :param threshold: upper bounds on how large the sum filesize of a batch can
        be.

    :return: List of Batches containing all files passed in.
    """
    process_logger = ProcessLogger("batch_files", threshold=threshold)
    process_logger.log_start()

    ongoing_batches = {t: Batch(t) for t in ConfigType}
    batch_count = {str(t): 0 for t in ConfigType}

    # iterate over file tuples, and add them to the ongoing batches. if a batch
    # is going to go past the threshold limit, move it to complete batches, and
    # create a new batch.
    filecount = 0
    for (filename, size) in files:
        try:
            config_type = ConfigType.from_filename(filename)

            filecount += 0
            batch = ongoing_batches[config_type]

            if (
                batch.total_size + int(size) > threshold
                and len(batch.filenames) > 0
            ):
                yield batch

                ongoing_batches[config_type] = Batch(config_type)
                batch = ongoing_batches[config_type]
                batch_count[str(config_type)] += 1

            batch.add_file(filename, int(size))
        except ConfigTypeFromFilenameException as config_exception:
            logging.warning(config_exception)
        except Exception as exception:
            logging.exception(
                "failed=%s, error_type=%s",
                "get_config_from_filename",
                type(exception).__name__,
            )
            continue

    # add the ongoing batches too complete ones and return.
    for (_, batch) in ongoing_batches.items():
        if batch.total_size > 0:
            yield batch
            batch_count[str(config_type)] += 1

    process_logger.add_metadata(**batch_count)
    process_logger.log_complete()
