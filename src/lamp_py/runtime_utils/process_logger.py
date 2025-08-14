import logging
import os
import time
import traceback
import uuid
import shutil
from typing import Any, Dict, Union, Optional

import psutil

MdValues = Optional[Union[str, int, float, bool]]


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

        self.add_metadata(**metadata)

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

        if self.default_data.get("status") is not None and print_log:
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
        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "failed"
        self.default_data["duration"] = f"{duration:.2f}"
        self.default_data["error_type"] = type(exception).__name__

        # This is for exceptions that are not 'raised'
        # 'raised' exceptions will also be logged to sys.stderr
        for tb in traceback.format_tb(exception.__traceback__):
            for line in tb.strip("\n").split("\n"):
                logging.error(f"uuid={self.default_data["uuid"][-12:]}, {line.strip('\n')}")

        # Log Exception
        for line in traceback.format_exception_only(exception):
            logging.error(f"uuid={self.default_data["uuid"][-12:]}, {line.strip('\n')}")

        # Log Process Failure
        logging.exception(self._get_log_string())
