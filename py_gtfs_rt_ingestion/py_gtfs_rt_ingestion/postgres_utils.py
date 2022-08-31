import os

import boto3
import psycopg2

from .error import AWSException
from .logging_utils import ProcessLogger


def get_psql_conn() -> psycopg2.extensions.connection:
    """get a connection to the postgres db"""
    process_logger = ProcessLogger("connect_to_db")
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

        process_logger.add_metadata(
            db_name=db_name,
            db_user=db_user,
            db_host=db_host,
            db_port=db_port,
        )

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

            process_logger.add_metadata(db_ssl_cert=db_ssl_cert)
            assert os.path.isfile(db_ssl_cert)

        process_logger.log_start()

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
        process_logger.log_failure(exception)
        raise AWSException("Unable to Connect to DataBase") from exception


def insert_metadata(written_file) -> None:  # type: ignore
    """
    add a row to metadata table containing the filepath and its status as
    unprocessed
    """
    filepath = written_file.path
    process_logger = ProcessLogger("metadata_insert", filepath=filepath)
    process_logger.log_start()

    try:
        with get_psql_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'INSERT INTO "metadataLog" (processed, path) VALUES (%s, %s)',
                    (False, filepath),
                )
        process_logger.log_complete()
    except Exception as exception:
        process_logger.log_failure(exception)
