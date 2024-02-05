import time
import os
import sys
from multiprocessing import Process
from queue import Queue
from typing import Any, Optional

import boto3

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


def running_in_aws() -> bool:
    """
    return True if running on aws, else False
    """
    return bool(os.getenv("AWS_DEFAULT_REGION"))


def check_for_parallel_tasks() -> None:
    """
    Check that that this task is not already running on ECS
    """
    if not running_in_aws():
        return

    process_logger = ProcessLogger("check_for_tasks")
    process_logger.log_start()

    client = boto3.client("ecs")
    ecs_cluster = os.environ["ECS_CLUSTER"]
    ecs_task_group = os.environ["ECS_TASK_GROUP"]

    try:
        # get all of the tasks running on the cluster
        task_arns = client.list_tasks(cluster=ecs_cluster)["taskArns"]

        # if tasks are running on the cluster, get their descriptions and check to
        # count matches the ecs task group.
        match_count = 0
        if task_arns:
            running_tasks = client.describe_tasks(
                cluster=ecs_cluster, tasks=task_arns
            )["tasks"]

            for task in running_tasks:
                if ecs_task_group == task["group"]:
                    match_count += 1

        # if the group matches, raise an exception that will terminate the process
        if match_count > 1:
            raise SystemError(
                f"Multiple {ecs_task_group} ECS Tasks Running in {ecs_cluster}"
            )

    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception

    process_logger.log_complete()
