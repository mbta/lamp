import logging
import os
import time
import traceback
import uuid
import shutil
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any, Dict, Union, Optional, List, Generator

import dataframely as dy
import psutil
from dataframely.exc import ValidationError

from lamp_py.runtime_utils.remote_files import data_validation_errors

MdValues = Optional[Union[str, int, float, bool, date, BaseException, List[str]]]


class ProcessLogger:
    """
    Class to help with logging events that happen inside of a function.
    """

    # default_data keys that can not be added as metadata
    protected_keys = [
        "parent",
        "process_name",
        "process_id",
        "uuid",
        "status",
        "duration",
        "error_type",
        "free_disk_mb",
        "free_mem_pct",
        "print_log",
    ]

    def __init__(self, process_name: str, **metadata: MdValues) -> None:
        """
        create a process logger with a name and optional metadata. a start time
        and uuid will be created for timing and unique identification
        """
        logging.getLogger().setLevel("INFO")

        self.default_data: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}

        self.default_data["parent"] = os.environ.get("SERVICE_NAME", "unknown")
        self.default_data["process_name"] = process_name

        self.start_time = 0.0

        self.add_metadata(**metadata, print_log=False)  # wait to start the logger

    def _get_log_string(self) -> str:
        """create logging string for log write"""
        _, _, free_disk_bytes = shutil.disk_usage("/")
        used_mem_pct = psutil.virtual_memory().percent
        self.default_data["free_disk_mb"] = int(free_disk_bytes / (1000 * 1000))
        self.default_data["free_mem_pct"] = int(100 - used_mem_pct)
        logging_list = []
        # add default data to log output
        for key, value in self.default_data.items():
            logging_list.append(f"{key}={value}")

        # add metadata to log output
        for key, value in self.metadata.items():
            logging_list.append(f"{key}={value}")

        return ", ".join(logging_list)

    def _start_if_unstarted(self) -> None:
        try:
            self.default_data["uuid"]
        except KeyError:
            self.log_start()

    def add_metadata(self, **metadata: MdValues) -> None:
        """
        add metadata to the process logger

        :param print_log: if True(default), print log after metadata is added
        """
        metadata.setdefault("print_log", True)
        print_log = bool(metadata.get("print_log"))
        for key, value in metadata.items():
            # skip metadata key if protected as default_data key
            # maybe raise on this? instead of fail silently
            if key in ProcessLogger.protected_keys:
                continue
            self.metadata[str(key)] = str(value)

        if print_log:
            if self.default_data.get("status") is None:
                self._start_if_unstarted()
            if self.default_data.get("status") is not None:
                self.default_data["status"] = "add_metadata"
                logging.info(self._get_log_string())

    def log_start(self) -> None:
        """log the start of a proccess"""
        self.default_data["uuid"] = uuid.uuid4()
        self.default_data["process_id"] = os.getpid()
        self.default_data["status"] = "started"
        self.default_data.pop("duration", None)
        self.default_data.pop("error_type", None)

        self.start_time = time.monotonic()

        logging.info(self._get_log_string())

    def log_complete(self) -> None:
        """log the completion of a proccess with duration"""
        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "complete"
        self.default_data["duration"] = f"{duration:.2f}"

        logging.info(self._get_log_string())

    def log_failure(self, exception: Exception) -> None:
        """log the failure of a process with exception type"""
        self._start_if_unstarted()

        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "failed"
        self.default_data["duration"] = f"{duration:.2f}"
        self.default_data["error_type"] = type(exception).__name__

        # This is for exceptions that are not 'raised'
        # 'raised' exceptions will also be logged to sys.stderr
        for tb in traceback.format_tb(exception.__traceback__):
            for line in tb.strip("\n").split("\n"):
                logging.error(f"uuid={self.default_data["uuid"]}, {line.strip('\n')}")

        # Log Exception
        for line in traceback.format_exception_only(exception):
            logging.error(f"uuid={self.default_data["uuid"]}, {line.strip('\n')}")

        # Log Process Failure
        has_exception_info = bool(exception.__traceback__)
        logging.exception(self._get_log_string(), exc_info=has_exception_info)

    def log_warning(self, exception: Exception) -> None:
        "Log a non-critical exception as a warning."
        self._start_if_unstarted()

        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "warned"
        self.default_data["duration"] = f"{duration:.2f}"
        self.default_data["error_type"] = type(exception).__name__

        for tb in traceback.format_exception_only(exception):
            for line in tb.strip("\n").split("\n"):
                logging.warning(f"uuid={self.default_data["uuid"]}, {line.strip('\n')}")
        logging.warning(self._get_log_string())

    def log_dataframely_filter_results(
        self, valid: dy.DataFrame, invalid: dy.FailureInfo, log_level: Optional[int] = logging.WARNING
    ) -> dy.DataFrame:
        """
        Log valid records and validation errors from dataframely filtering, write invalid records to the error bucket, and return valid records.

        :param valid: The first element from a dy.Schema filter call, representing the valid records filtered from the input Frame.
        :param invalid: The second element from a dy.Schema filter call, representing the failure information captured by Dataframely.
        :param log_level: Whether to treat validation errors as warnings (the default), or errors.

        :return valid: The valid records.
        """

        self.add_metadata(valid_records=valid.height, invalid_records=invalid.invalid().height if invalid else 0)

        for exception in _get_validation_errors(invalid):
            if log_level == logging.WARNING:
                self.log_warning(exception)
            elif log_level == logging.ERROR:
                self.log_failure(exception)

            bucket_path = (
                data_validation_errors.s3_uri
                if bool(
                    os.getenv("AWS_DEFAULT_REGION")
                )  # checks if running on AWS; can't import from lamp_py.aws.ecs due to circularity
                else Path(os.getenv("TEMP_DIR", default="/tmp")).joinpath("validation_errors").as_uri()
            )
            error_file = os.path.join(
                bucket_path,
                self.default_data["process_name"],
                str(self.default_data["uuid"]) + ".parquet",
            )

            invalid.write_parquet(error_file)

        return valid


@contextmanager
def override_log_level(set_log_level: int) -> Generator[None, Any, None]:
    """
    Context manager to temporarily suppress all logging to a certain level.

    if set_log_level is logging.CRITICAL, all logging is suppressed
    will always return to the originally set level upon exiting context
    """
    # Store the current state
    previous_level = logging.root.manager.disable
    try:
        # Disables all logging at set_log_level or below
        logging.disable(set_log_level)
        yield
    finally:
        # Restore the previous state
        logging.disable(previous_level)


def _get_validation_errors(invalid: dy.FailureInfo) -> list[ValidationError]:
    "Returns the types of validation errors present in the FailureInfos."
    return [ValidationError(f"error_type={k}, error_records={v}") for k, v in invalid.counts().items()]
