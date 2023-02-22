import os
import sys
import time
import logging

from multiprocessing import Queue
from typing import Any


def handle_ecs_sigterm(_: int, __: Any) -> None:
    """
    handler function for when ECS recieves ECS SIGTERM
    """
    logging.info("AWS ECS SIGTERM received")
    os.environ["GOT_SIGTERM"] = "TRUE"


def check_for_sigterm(metadata_queue: Queue) -> None:
    """
    check if SIGTERM recived from ECS. If found, terminate process.
    """
    if os.environ.get("GOT_SIGTERM") is not None:
        logging.info("SIGTERM received, terminating process...")
        metadata_queue.put(None)
        time.sleep(5)
        sys.exit()
