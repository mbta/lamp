import os
import platform
from typing import Any, Tuple, Dict
import urllib.parse as urlparse
from multiprocessing import Process, Queue

import boto3
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .logging_utils import ProcessLogger

SqlBase: Any = declarative_base()


class MetadataLog(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for keeping track of parquet files in S3"""

    __tablename__ = "metadataLog"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    processed = sa.Column(sa.Boolean, default=sa.false())
    path = sa.Column(sa.String(256), nullable=False, unique=True)
    created_on = sa.Column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )


def get_db_password() -> str:
    """
    function to provide rds password

    used to refresh auth token, if required
    """
    db_password = os.environ.get("DB_PASSWORD", None)
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_user = os.environ.get("DB_USER")
    db_region = os.environ.get("DB_REGION", None)

    if db_password is None:
        # generate aws db auth token
        client = boto3.client("rds")
        return urlparse.quote_plus(
            client.generate_db_auth_token(
                DBHostname=db_host,
                Port=db_port,
                DBUsername=db_user,
                Region=db_region,
            )
        )

    return db_password


def postgres_event_update_db_password(
    _: sa.engine.interfaces.Dialect,
    __: Any,
    ___: Tuple[Any, ...],
    cparams: Dict[str, Any],
) -> None:
    """
    update database passord on every new connection attempt
    this will refresh db auth token passwords
    """
    cparams["password"] = get_db_password()


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
        db_user = os.environ.get("DB_USER")
        db_ssl_options = ""

        # when using docker, the db host env var will be "local_rds" but
        # accessed via the "0.0.0.0" ip address (mac specific)
        if db_host == "local_rds" and "macos" in platform.platform().lower():
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
            db_password = get_db_password()

            assert db_password is not None
            assert db_password != ""

            # set the ssl cert path to the file that should be added to the
            # ecs container at deploy time
            db_ssl_cert = os.path.abspath(
                os.path.join("/", "usr", "local", "share", "amazon-certs.pem")
            )

            assert os.path.isfile(db_ssl_cert)

            # update the ssl options string to add to the database url
            db_ssl_options = f"?sslmode=verify-full&sslrootcert={db_ssl_cert}"

        database_url = (
            f"postgresql+psycopg2://{db_user}:"
            f"{db_password}@{db_host}/{db_name}"
            f"{db_ssl_options}"
        )

        engine = sa.create_engine(
            database_url,
            echo=echo,
            future=True,
            pool_recycle=300,
            pool_pre_ping=True,
        )

        process_logger.log_complete()
        return engine
    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception


def _rds_writer_process(metadata_queue: Queue) -> None:
    """
    process for writing matadata paths recieved from metadata_queue

    if None recieved from queue, process will exit
    """
    engine = get_local_engine()

    sa.event.listen(
        engine,
        "do_connect",
        postgres_event_update_db_password,
    )

    session = sessionmaker(bind=engine)

    while True:
        metadata = metadata_queue.get()

        if metadata is None:
            break

        metadata_path = metadata.path
        insert_statement = sa.insert(MetadataLog.__table__).values(
            processed=False, path=metadata_path
        )
        process_logger = ProcessLogger(
            "metadata_insert", filepath=metadata_path
        )
        process_logger.log_start()
        try:
            # Instance of 'sessionmaker' has no 'begin' member (no-member)
            with session.begin() as cursor:  # pylint: disable=E1101
                cursor.execute(insert_statement)
            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)


def start_rds_writer_process() -> Queue:
    """
    create metadata queue and rds writer process

    return metadata queue
    """
    metadata_queue: Queue = Queue()

    writer_process = Process(target=_rds_writer_process, args=(metadata_queue,))
    writer_process.start()

    return metadata_queue
