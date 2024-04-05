import logging
import os
import time
import uuid
import shutil
import psutil
from typing import Any, Dict, Union, Optional

MdValues = Optional[Union[str, int, float]]


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
        "free_mem_mb",
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
        free_mem_bytes = psutil.virtual_memory().available
        self.default_data["free_disk_mb"] = int(free_disk_bytes / (1000*1000))
        self.default_data["free_mem_mb"] = int(free_mem_bytes / (1000*1000))
        logging_list = []
        # add default data to log output
        for key, value in self.default_data.items():
            logging_list.append(f"{key}={value}")

        # add metadata to log output
        for key, value in self.metadata.items():
            logging_list.append(f"{key}={value}")

        return ", ".join(logging_list)

    def add_metadata(self, **metadata: MdValues) -> None:
        """add metadata to the process logger"""
        for key, value in metadata.items():
            # skip metadata key if protected as default_data key
            # maybe raise on this? instead of fail silently
            if key in ProcessLogger.protected_keys:
                continue
            self.metadata[str(key)] = str(value)

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
        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "failed"
        self.default_data["duration"] = f"{duration:.2f}"
        self.default_data["error_type"] = type(exception).__name__

        logging.exception(self._get_log_string())
