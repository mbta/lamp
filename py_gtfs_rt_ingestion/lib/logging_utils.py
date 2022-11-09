import logging
import time
import uuid
import os

from typing import Union, Optional

MdValues = Union[str, int, float]


class ProcessLogger:
    """
    Class to help with logging events that happen inside of a function.
    """

    def __init__(self, process_name: str, **metadata: MdValues) -> None:
        """
        create a process logger with a name and optional metadata. a start time
        and uuid will be created for timing and unique identification
        """
        self.parent = "gtfs_ingestion"
        self.process_name = process_name
        self.metadata = metadata
        self.uuid: Optional[uuid.UUID] = None
        self.start_time = 0.0
        self.process_id = 0

    def add_metadata(self, **metadata: MdValues) -> None:
        """add metadata to the process logger"""
        for key, value in metadata.items():
            self.metadata[key] = value

    def log_start(self) -> None:
        """log the start of a proccess"""
        self.uuid = uuid.uuid4()
        self.start_time = time.monotonic()
        self.process_id = os.getpid()
        logging_string = (
            f"parent={self.parent}, "
            f"start={self.process_name}, "
            f"uuid={self.uuid}, "
            f"process_id={self.process_id}"
        )

        if self.metadata:
            metadata_string = ", ".join(
                [f"{key}={value}" for (key, value) in self.metadata.items()]
            )
            logging_string += f", {metadata_string}"
        logging.info(logging_string)

    def log_complete(self) -> None:
        """log the completion of a proccess with duration"""
        duration = time.monotonic() - self.start_time
        logging_string = (
            f"parent={self.parent}, "
            f"complete={self.process_name}, "
            f"uuid={self.uuid}, "
            f"process_id={self.process_id}, "
            f"duration={duration:.2f}"
        )

        if self.metadata:
            metadata_string = ", ".join(
                [f"{key}={value}" for (key, value) in self.metadata.items()]
            )
            logging_string += f", {metadata_string}"
        logging.info(logging_string)

    def log_failure(self, exception: Exception) -> None:
        """log the failure of a process with exception details"""
        duration = time.monotonic() - self.start_time
        logging_string = (
            f"parent={self.parent}, "
            f"failed={self.process_name}, "
            f"uuid={self.uuid}, "
            f"process_id={self.process_id}, "
            f"duration={duration:.2f}, "
            f"error_type={type(exception).__name__}"
        )

        if self.metadata:
            metadata_string = ", ".join(
                [f"{key}={value}" for (key, value) in self.metadata.items()]
            )
            logging_string += f", {metadata_string}"
        logging.exception(logging_string)
