import logging
import os
import sys
from multiprocessing import Process, Queue
from typing import Any, Optional


def handle_ecs_sigterm(_: int, __: Any) -> None:
    """
    handler function for when ECS recieves ECS SIGTERM
    """
    logging.info("AWS ECS SIGTERM received")
    os.environ["GOT_SIGTERM"] = "TRUE"


def check_for_sigterm(
    metadata_queue: Optional[Queue] = None,
    rds_process: Optional[Process] = None,
) -> None:
    """
    check if SIGTERM recived from ECS. If found, terminate process.
    """
    if os.environ.get("GOT_SIGTERM") is not None:
        logging.info("SIGTERM received, terminating process...")

        # send signal to stop rds writer process and wait for exit
        if metadata_queue is not None:
            metadata_queue.put(None)
        if rds_process is not None:
            rds_process.join()

        sys.exit()
