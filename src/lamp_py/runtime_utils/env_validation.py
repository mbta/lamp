import os
from typing import Iterable, List, Optional

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.__version__ import VERSION


def validate_environment(
    required_variables: List[str],
    private_variables: Optional[List[str]] = None,
    optional_variables: Optional[List[str]] = None,
    db_prefixes: Iterable[str] = (),
) -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    process_logger = ProcessLogger("validate_env")
    process_logger.log_start()

    if private_variables is None:
        private_variables = []

    metadata = {"lamp_version": VERSION}

    # every pipeline needs a service name for logging
    required_variables.append("SERVICE_NAME")

    # add required database variables
    for prefix in db_prefixes:
        required_variables += [
            f"{prefix}_DB_HOST",
            f"{prefix}_DB_NAME",
            f"{prefix}_DB_PORT",
            f"{prefix}_DB_USER",
        ]
        # if db password is missing, db region is required to generate a
        # token to use as the password to the cloud database
        if os.environ.get(f"{prefix}_DB_PASSWORD", None) is None:
            required_variables.append("DB_REGION")

    # check for missing variables. add found variables to our logs.
    missing_required = []
    for key in required_variables:
        value = os.environ.get(key, None)
        if value is None:
            missing_required.append(key)

        # do not log private variables
        if key in private_variables:
            value = "**********"
        assert isinstance(value, str)  # assert value is not none for type safety
        metadata[key] = value

    # for optional variables, access ones that exist and add them to logs.
    if optional_variables:
        for key in optional_variables:
            value = os.environ.get(key, None)
            if value is not None:
                # do not log private variables
                if key in private_variables:
                    value = "**********"
                metadata[key] = value

    process_logger.add_metadata(**metadata)

    # if required variables are missing, log a failure and throw.
    if missing_required:
        exception = EnvironmentError(f"Missing required environment variables {missing_required}")
        process_logger.log_failure(exception)
        raise exception

    process_logger.log_complete()
