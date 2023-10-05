import os
from typing import List, Optional

from .process_logger import ProcessLogger


def validate_environment(
    required_variables: List[str],
    optional_variables: Optional[List[str]] = None,
    validate_db: bool = False,
) -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    process_logger = ProcessLogger("validate_env")
    process_logger.log_start()

    # every pipeline needs a service name for logging
    required_variables.append("SERVICE_NAME")

    # add required database variables
    if validate_db:
        required_variables += [
            "DB_HOST",
            "DB_NAME",
            "DB_PORT",
            "DB_USER",
        ]

    # check for missing variables. add found variables to our logs.
    missing_required = []
    for key in required_variables:
        value = os.environ.get(key, None)
        if value is None:
            missing_required.append(key)
        process_logger.add_metadata(**{key: value})

    # if db password is missing, db region is required to generate a token to
    # use as the password to the cloud database
    if validate_db:
        if os.environ.get("DB_PASSWORD", None) is None:
            value = os.environ.get("DB_REGION", None)
            if value is None:
                missing_required.append("DB_REGION")
            process_logger.add_metadata(DB_REGION=value)

    # for optional variables, access ones that exist and add them to logs.
    if optional_variables:
        for key in optional_variables:
            value = os.environ.get(key, None)
            if value is not None:
                process_logger.add_metadata(**{key: value})

    # if required variables are missing, log a failure and throw.
    if missing_required:
        exception = EnvironmentError(
            f"Missing required environment variables {missing_required}"
        )
        process_logger.log_failure(exception)
        raise exception

    process_logger.log_complete()