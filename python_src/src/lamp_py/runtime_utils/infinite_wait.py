import logging
import time

from lamp_py.aws.ecs import check_for_sigterm


def infinite_wait(reason: str) -> None:
    """
    When running on ECS, propagating an exception up the call stack and killing
    the processes will result in the process being restarted, to keep the task
    count at one. This method should be called instead when we want to pause
    the process for intervention before restarting.
    """
    # amount of time to sleep between logging statements
    sleep_time = 60
    count = 0

    while True:
        check_for_sigterm()

        # log every ten minutes
        if count == 10:
            logging.error("Pausing for %s", reason)
            count = 0

        # sleep
        time.sleep(sleep_time)
        count += 1
