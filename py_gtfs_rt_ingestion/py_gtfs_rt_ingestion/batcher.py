import logging
import os
import sys
import json

from collections.abc import Iterable
from typing import List, Tuple

from .converter import ConfigType
from .error import (
    AWSException,
    ArgumentException,
    ConfigTypeFromFilenameException,
)
from .s3_utils import invoke_async_lambda


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
        if (
            self.prefix != ""
            and filename.startswith(self.prefix)
            and filename.endswith(self.suffix)
        ):
            filename = filename.removeprefix(self.prefix)
            filename = filename.removesuffix(self.suffix)
            self.filenames.append(filename)

        else:
            full_filenames = unpack_filenames(
                prefix=self.prefix, suffix=self.suffix, filenames=self.filenames
            )
            full_filenames.append(filename)

            self.prefix, self.suffix, self.filenames = compress_filenames(
                full_filenames
            )

        self.total_size += filesize

    def trigger_lambda(self) -> None:
        """
        Invoke the ingestion lambda that will take the files in this batch and
        convert them into a parquette file.
        """
        try:
            logging.info(
                "Invoking Ingestion Lambda (%.4f mb %s files)",
                self.total_size / 1000000,
                len(self.filenames),
            )
            function_arn = os.environ["INGEST_FUNCTION_ARN"]
            invoke_async_lambda(
                function_arn=function_arn, event=self.create_event()
            )
        except KeyError as e:
            raise ArgumentException("Ingest Func Arn not Defined") from e
        except Exception as e:
            raise AWSException("Unable To Trigger Ingest Lambda") from e

    def create_event(self) -> dict:
        """
        Create an event object that will be used in the invocation of the
        ingestion lambda
        """
        return {
            "prefix": self.prefix,
            "suffix": self.suffix,
            "filenames": self.filenames,
        }

    def event_size_over_limit(self) -> bool:
        """
        Returns True if event size is over limit of 245kb, otherwise False
        """
        event_size_kb = sys.getsizeof(json.dumps(self.create_event())) / 1000
        if event_size_kb >= 245:
            logging.info("Event size of %d kb is over limit.", event_size_kb)
            return True
        return False


def compress_filenames(filenames: List[str]) -> Tuple[str, str, List[str]]:
    """
    compress a list of filenames into a dict with the prefix, suffix, and list
    of unique components
    """
    if len(filenames) == 0:
        return ("", "", [])

    def longest_common(filenames: List[str], prefix: bool = True) -> str:
        """
        get the longest start or end string common to all files in filenames
        """
        first_file = filenames[0]

        def get_stem(i: int) -> str:
            """get the 0->ith or ith->-1 string from first_file"""
            if prefix:
                return first_file[:i]
            # if suffix
            return first_file[-i:]

        def is_common(file: str, stem: str) -> bool:
            """do all files start or end with this string"""
            if prefix:
                return file.startswith(stem)
            # if suffix
            return file.endswith(stem)

        result = ""

        for i in range(1, len(first_file)):
            stem = get_stem(i)

            for file in filenames:
                if not is_common(file, stem):
                    return result

            result = stem

        return result

    prefix = longest_common(filenames=filenames, prefix=True)
    filenames = [file.removeprefix(prefix) for file in filenames]

    suffix = longest_common(filenames=filenames, prefix=False)
    filenames = [file.removesuffix(suffix) for file in filenames]

    return (prefix, suffix, filenames)


def unpack_filenames(
    prefix: str, suffix: str, filenames: List[str]
) -> List[str]:
    """
    convert a compressed dict of directories and filenames into a list of
    filenames for ingestion
    """
    combined = []
    for unique in filenames:
        combined.append(prefix + unique + suffix)

    return combined


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

        # lambda async even payload size is limited to 256KB
        # https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
        # use sys.getsizeof on batch event to get general size of payload
        # limit estimated payload size to 245KB for now to account for
        # additional request overhead and measurement inaccuracy
        if (
            batch.total_size + int(size) > threshold
            or batch.event_size_over_limit()
        ) and len(batch.filenames) > 0:
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
