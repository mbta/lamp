import os
import urllib.parse as urlparse

import boto3
import psycopg2
import sqlalchemy as sa

from .error import AWSException
from .logging_utils import ProcessLogger


def get_local_engine(echo: bool = False) -> sa.engine.Engine:
    """
    Get an SQL Alchemy engine that connects to a locally Postgres RDS stood up
    via docker using env variables
    """
    process_logger = ProcessLogger("create_sql_engine")
    process_logger.log_start()
    try:
        db_host = os.environ.get("DB_HOST")
        db_name = os.environ.get("DB_NAME")
        db_password = os.environ.get("DB_PASSWORD", None)
        db_port = os.environ.get("DB_PORT")
        db_region = os.environ.get("DB_REGION", None)
        db_user = os.environ.get("DB_USER")
        db_ssl_options = ""

        # when using docker, the db host env var will be "local_rds" but
        # accessed via the "0.0.0.0" ip address
        if db_host == "local_rds":
            db_host = "0.0.0.0"

        assert db_host is not None
        assert db_name is not None
        assert db_port is not None
        assert db_user is not None

        process_logger.add_metadata(
            host=db_host, database_name=db_name, user=db_user, port=db_port
        )

        # use presence of password as indicator of connection type.
        #
        # if its not provided, assume cloud database where ssl is used and
        # passwords are generated on the fly
        #
        # if it is provided, assume local docker database
        if db_password is None:
            # spin up a rds client to get the db password
            client = boto3.client("rds")
            db_password = urlparse.quote_plus(
                client.generate_db_auth_token(
                    DBHostname=db_host,
                    Port=db_port,
                    DBUsername=db_user,
                    Region=db_region,
                )
            )

            assert db_password is not None
            assert db_password != ""

            # set the ssl cert path to the file that should be added to the
            # lambda function at deploy time
            db_ssl_cert = os.path.abspath(
                os.path.join(
                    os.path.abspath(__file__), "..", "..", "aws-cert-bundle.pem"
                )
            )

            assert os.path.isfile(db_ssl_cert)

            # update the ssl options string to add to the database url
            db_ssl_options = f"?sslmode=verify-full&sslrootcert={db_ssl_cert}"

        database_url = (
            f"postgresql+psycopg2://{db_user}:"
            f"{db_password}@{db_host}/{db_name}"
            f"{db_ssl_options}"
        )

        engine = sa.create_engine(database_url, echo=echo, future=True)

        process_logger.log_complete()
        return engine
    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception


def get_psql_conn() -> psycopg2.extensions.connection:
    """get a connection to the postgres db"""
    process_logger = ProcessLogger("connect_to_db")
    try:
        db_host = os.environ.get("DB_HOST")
        db_name = os.environ.get("DB_NAME")
        db_password = os.environ.get("DB_PASSWORD", None)
        db_port = os.environ.get("DB_PORT")
        db_region = os.environ.get("DB_REGION", None)
        db_user = os.environ.get("DB_USER")
        db_ssl_cert = None
        ssl_mode = "prefer"

        assert db_host is not None
        assert db_name is not None
        assert db_port is not None
        assert db_user is not None

        # when using docker, the db host env var will be "local_rds" but
        # accessed via the "0.0.0.0" ip address
        if db_host == "local_rds":
            db_host = "0.0.0.0"

        if db_password is None:
            # spin up a rds client to get the db password
            client = boto3.client("rds")
            db_password = urlparse.quote_plus(
                client.generate_db_auth_token(
                    DBHostname=db_host,
                    Port=db_port,
                    DBUsername=db_user,
                    Region=db_region,
                )
            )

            assert db_password is not None
            assert db_password != ""

            # set the ssl cert path to the file that should be added to the
            # lambda function at deploy time
            db_ssl_cert = os.path.abspath(
                os.path.join(
                    os.path.abspath(__file__), "..", "..", "aws-cert-bundle.pem"
                )
            )

            assert os.path.isfile(db_ssl_cert)
            ssl_mode = "verify-full"

            process_logger.add_metadata(db_ssl_cert=db_ssl_cert)

        process_logger.add_metadata(
            db_host=db_host,
            db_name=db_name,
            db_port=db_port,
            db_user=db_user,
            ssl_mode=ssl_mode,
        )
        process_logger.log_start()

        conn = psycopg2.connect(
            dbname=db_name,
            password=db_password,
            user=db_user,
            host=db_host,
            port=db_port,
            sslmode=ssl_mode,
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

    metadata_table = sa.Table(
        "metadataLog",
        sa.MetaData(),
        sa.Column("pk_id", sa.Integer, primary_key=True),
        sa.Column("processed", sa.Boolean, default=sa.false()),
        sa.Column("path", sa.String(256), nullable=False, unique=True),
        sa.Column(
            "created_on",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
    )

    try:
        with get_local_engine().connect() as connection:
            stmt = sa.insert(metadata_table).values(
                processed=False, path=filepath
            )
            connection.execute(stmt)
            connection.commit()
        process_logger.log_complete()
    except Exception as exception:
        process_logger.log_failure(exception)
