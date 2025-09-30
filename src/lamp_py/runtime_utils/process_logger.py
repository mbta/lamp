import logging
import os
import time
import traceback
import uuid
import shutil
from datetime import date
from typing import Any, Dict, Union, Optional, List, Tuple

import dataframely as dy
import psutil

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

    def log_dataframely_filter_results(
        self,
        schema_filter: Tuple[dy.DataFrame, dy.FailureInfo],
        *filter_results: Tuple[Union[dy.DataFrame, dy.LazyFrame, dy.Collection], Union[dy.FailureInfo, Any]],
    ) -> dy.DataFrame:
        """
        Log results of .filter method on dataframely Schema and return its valid DataFrame.
        Log results of additional filters but do not return their contents.
        """

        valid_records = schema_filter[0].height

        validation_error_count = _sum_validation_errors(schema_filter[1], *[f[1] for f in filter_results])

        self.add_metadata(valid_records=valid_records, validation_errors=validation_error_count)

        validation_error_list = _list_validation_errors(schema_filter[1], *[f[1] for f in filter_results])

        if validation_error_list:
            self.log_failure(dy.exc.ValidationError(", ".join(validation_error_list)))

        return schema_filter[0]


def _sum_validation_errors(*invalids: Union[dy.FailureInfo, Any]) -> int:
    """
    Sum all count values from schema and collection validations.

    Args:
        *objects: Variable number of arguments, each can be either:
                 - dy.FailureInfo
                 - A dictionary of dy.FailureInfo objects

    Returns:
        Total sum of all count values
    """
    total = 0

    for obj in invalids:
        if not obj:
            continue  # ignore empty dicts
        # Check if it's a collection or a single object
        if isinstance(obj, dy.FailureInfo):
            # It's a single object - sum its counts directly
            total += sum(obj.counts().values())
        else:
            # It's a collection - sum counts from all items in it
            for item in obj.values():
                total += sum(item.counts().values())
    return total


def _list_validation_errors(*invalids: Union[dy.FailureInfo, Any]) -> List[str]:
    "Returns the types of validation errors present in the FailureInfos."
    validation_errors: List[str] = []

    for obj in invalids:
        if not obj:
            continue  # ignore empty dicts
        # Check if it's a collection or a single object
        if isinstance(obj, dy.FailureInfo):
            # It's a single object - add its validation errors directly
            validation_errors += obj.counts().keys()
        else:
            # It's a collection - add validation errors from all its items
            for item in obj.values():
                validation_errors += item.counts().keys()
    return list(set(validation_errors)) # deduplicate
