import logging
import os
import time

import boto3
import psycopg2

from .error import AWSException


def get_psql_conn() -> psycopg2.extensions.connection:
    """get a connection to the postgres db"""
    try:
        db_host = os.environ.get("DB_HOST", None)
        db_name = os.environ.get("DB_NAME", None)
        db_password = os.environ.get("DB_PASSWORD", None)
        db_port = os.environ.get("DB_PORT", None)
        db_user = os.environ.get("DB_USER", None)
        db_region = os.environ.get("DB_REGION", None)
        db_ssl_cert = None

        # when using docker, the db host env var will be "local_rds" but
        # accessed via the "0.0.0.0" ip address
        if db_host == "local_rds":
            db_host = "0.0.0.0"

        if db_password is None:
            # spin up a rds client to get the db password
            client = boto3.client("rds")
            db_password = client.generate_db_auth_token(
                DBHostname=db_host,
                Port=db_port,
                DBUsername=db_user,
                Region=db_region,
            )

            # set the ssl cert path to the file that should be added to the
            # lambda function at deploy time
            db_ssl_cert = os.path.join(
                os.path.abspath(__file__), "..", "aws-cert-bundle.pem"
            )

            assert os.path.isfile(db_ssl_cert)

        logging.info(
            "start=%s, db_name=%s, db_user=%s, db_host=%s, db_port=%s",
            "connect_to_db",
            db_name,
            db_user,
            db_host,
            db_port,
        )

        conn = psycopg2.connect(
            dbname=db_name,
            password=db_password,
            user=db_user,
            host=db_host,
            port=db_port,
            sslrootcert=db_ssl_cert,
        )

        return conn
    except Exception as exception:
        logging.exception(
            "failed=%s, error_type=%s",
            "connect_to_db",
            type(exception).__name__,
        )
        raise AWSException("Unable to Connect to DataBase") from exception


def insert_metadata(written_file) -> None:  # type: ignore
    """
    add a row to metadata table containing the filepath and its status as
    unprocessed
    """
    filepath = written_file.path
    logging.info("start=%s, filepath=%s", "metadata_insert", filepath)
    start_time = time.time()

    try:
        with get_psql_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO "metadataLog" (processed, path) VALUES (%s, %s)',
                    (False, filepath),
                )
        logging.info(
            "complete=%s, duration=%.2f, filepath=%s",
            "metadata_insert",
            start_time - time.time(),
            filepath,
        )
    except Exception as exception:
        logging.exception(
            "failed=%s, error_type=%s, filepath=%s",
            "metadata_insert",
            type(exception).__name__,
            filepath,
        )
