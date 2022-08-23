import logging
import os
import psycopg2

from .error import AWSException


def get_psql_conn() -> psycopg2.extensions.connection:
    """get a connection to the postgres db"""
    try:
        db_name = os.environ.get("DB_NAME", None)
        db_user = os.environ.get("DB_USER", None)
        db_host = os.environ.get("DB_HOST", None)
        db_password = os.environ.get("DB_PASSWORD", None)
        db_port = os.environ.get("DB_PORT", None)

        # when using docker, the db host env var will be "local_rds" but
        # accessed via the "0.0.0.0" ip address
        if db_host == "local_rds":
            db_host = "0.0.0.0"

        conn = psycopg2.connect(
            dbname=db_name,
            password=db_password,
            user=db_user,
            host=db_host,
            port=db_port,
        )

        logging.info("Connecting to %s DataBase as %s", db_name, db_user)
        return conn
    except Exception as e:
        logging.error("Unable to connect to DataBase")
        logging.exception(e)
        raise AWSException("Unable to Connect to DataBase") from e


def insert_metadata(filepath: str) -> None:
    """
    add a row to metadata table containing the filepath and its status as
    unprocessed
    """
    logging.info("Adding Filepath to Metadata Table %s", filepath)

    try:
        with get_psql_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO "metadataLog" (processed, path) VALUES (%s, %s)',
                    (False, filepath),
                )
                logging.info("Added Filepath to Metadata Table %s", filepath)
    except Exception as error:
        logging.error("Unable to Add Filepath to Metadata Table %s", filepath)
        logging.exception(error)
