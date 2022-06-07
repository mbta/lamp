import logging
import os

from .config_base import ConfigType
from .error import (AWSException,
                    NoImplException,
                    ArgumentException,
                    ConfigTypeFromFilenameException)
from .s3_utils import invoke_async_lambda
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

    def __str__(self) -> None:
        return "Batch of %d bytes in %d %s files" % (self.total_size,
                                                     len(self.filenames),
                                                     self.config_type)

    def add_file(self, filename: str, filesize: int) -> None:
        self.filenames.append(filename)
        self.total_size += filesize

    def trigger_lambda(self) -> None:
        try:
            function_arn = os.environ["INGEST_FUNCTION_ARN"]
            invoke_async_lambda(function_arn=function_arn,
                                event=self.create_event())
        except KeyError as e:
            raise ArgumentException("Ingest Func Arn not Defined") from e
        except Exception as e:
            raise AWSException("Unable To Trigger Ingest Lambda") from e

    def create_event(self) -> dict:
        return {
            'files': self.filenames,
        }

def batch_files(files: Iterable[(str, int)], threshold: int) -> Iterable[Batch]:
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

        if batch.total_size + int(size) > threshold and len(batch.filenames) > 0:
            logging.info(batch)
            yield batch

            ongoing_batches[config_type] = Batch(config_type)
            batch = ongoing_batches[config_type]

        batch.add_file(filename, int(size))

    # add the ongoing batches too complete ones and return.
    for (_, batch) in ongoing_batches.items():
        if batch.total_size > 0:
            logging.info(batch)
            yield batch
