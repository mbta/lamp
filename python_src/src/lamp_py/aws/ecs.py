import time
import os
import sys
from multiprocessing import Process
from queue import Queue
from typing import Any, Optional

from lamp_py.runtime_utils.process_logger import ProcessLogger


def handle_ecs_sigterm(_: int, __: Any) -> None:
    """
    handler function for when ECS recieves ECS SIGTERM
    """
    process_logger = ProcessLogger("sigterm_received")
    process_logger.log_start()
    os.environ["GOT_SIGTERM"] = "TRUE"
    process_logger.log_complete()


def check_for_sigterm(
    metadata_queue: Optional[Queue[Optional[str]]] = None,
    rds_process: Optional[Process] = None,
) -> None:
    """
    check if SIGTERM recived from ECS. If found, terminate process.
    """
    if os.environ.get("GOT_SIGTERM") is not None:
        process_logger = ProcessLogger("stopping_ecs")
        process_logger.log_start()

        # send signal to stop rds writer process and wait for exit
        if metadata_queue is not None:
            metadata_queue.put(None)
        if rds_process is not None:
            rds_process.join()

        process_logger.log_complete()

        # delay for log statements to write before ecs death
        time.sleep(5)

        sys.exit()
