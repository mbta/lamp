import os
import sys
import logging

from typing import Any


def handle_ecs_sigterm(_: int, __: Any) -> None:
    """
    handler function for when ECS recieves ECS SIGTERM
    """
    logging.info("AWS ECS SIGTERM received")
    os.environ["GOT_SIGTERM"] = "TRUE"


def check_for_sigterm() -> None:
    """
    check if SIGTERM recived from ECS. If found, terminate process.
    """
    if os.environ.get("GOT_SIGTERM") is not None:
        logging.info("SIGTERM received, terminating process...")
        sys.exit()
