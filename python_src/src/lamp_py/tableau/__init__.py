"""Utilities for Interacting with Tableau and Hyper files"""

try:
    # pylint: disable=C0414
    #
    # Import alias does not rename original package. The intent is to grab it
    # here and pass it through other portions of the codebase.
    from .pipeline import start_parquet_updates as start_parquet_updates

    # pylint: enable=C0414

except ModuleNotFoundError as mfl_exception:
    import logging
    from lamp_py.postgres.postgres_utils import DatabaseManager

    # pylint: disable=W0613
    #
    # db_manaager is unused because this method has to match the function
    # signature of the method its replacing.
    def start_parquet_updates(db_manager: DatabaseManager) -> None:
        """
        re-implimentation of start parquet updates in the event that the
        tableauhyperapi module cannot be found.
        """
        logging.exception(
            "Unable to run parquet files on this machine due to Module Not Found error"
        )

    # pylint: enable=W0613
