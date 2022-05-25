import logging
import os

from .config_base import ConfigType
from .error import ConfigTypeFromFilenameException, NoImplException
from collections.abc import Iterable

class Batch(object):
    """
    will store a collection of filenames that should be downloaded and converted
    from json into parquet.
    """
    def __init__(self, config_type: ConfigType) -> None:
        self.config_type = config_type
        self.filenames = []
        self.total_size = 0
        self.bucket = 'mbta-gtfs-s3'

    def __str__(self) -> None:
        return "Batch of %d bytes in %d %s files" % (self.total_size,
                                                     len(self.filenames),
                                                     self.config_type)

    def add_file(self, filename: str, filesize: int) -> None:
        self.filenames.append(os.path.join('s3://',self.bucket,filename))
        self.total_size += filesize

    def trigger_lambda(self) -> None:
        raise NoImplException("Cannot Trigger Lambda on a Batch")

    def create_event(self) -> dict:
        return {
            'files': self.filenames,
        }

def batch_files(files: Iterable[(str, int)], threshold: int) -> list[Batch]:
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
    ongoing_batches = {t: Batch(t) for t in ConfigType}
    complete_batches = []

    # iterate over file tuples, and add them to the ongoing batches. if a batch
    # is going to go past the threshold limit, move it to complete batches, and
    # create a new batch.
    logging.info("Organizing files into batches.")
    for (filename, size) in files:
        try:
            config_type = ConfigType.from_filename(filename)
        except ConfigTypeFromFilenameException as config_exception:
            logging.warning(config_exception)
            continue
        except Exception as exception:
            logging.exception(exception)
            continue

        batch = ongoing_batches[config_type]

        if batch.total_size + int(size) > threshold:
            logging.info(batch)
            complete_batches.append(batch)
            ongoing_batches[config_type] = Batch(config_type)
            batch = ongoing_batches[config_type]

        batch.add_file(filename, int(size))

    # add the ongoing batches too complete ones and return.
    for (_, batch) in ongoing_batches.items():
        if batch.total_size > 0:
            logging.info(batch)
            complete_batches.append(batch)

    return complete_batches
