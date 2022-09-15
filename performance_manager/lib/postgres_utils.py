from typing import Dict, List, Any, Union
import json
import os
import pathlib
import urllib.parse as urlparse

import boto3
import pandas

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from .logging_utils import ProcessLogger
from .postgres_schema import MetadataLog, SqlBase

HERE = os.path.dirname(os.path.abspath(__file__))


def get_local_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:
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

        engine = sa.create_engine(database_url, echo=echo, future=True)

        process_logger.log_complete()
        return engine
    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception


def get_experimental_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:
    """
    return lightweight engine using local memeory that doens't require a
    database to be stood up. great for testing from within the shell.
    """
    engine = sa.create_engine(
        "sqlite+pysqlite:///./test.db",
        echo=echo,
        future=True
        # "sqlite+pysqlite:///:memory:", echo=echo, future=True
    )
    return engine


def get_aws_engine() -> sa.future.engine.Engine:
    """
    return an engine connected to our aws rds
    """
    # TODO(zap) - figure out how to connect to the AWS RDS for Writing


# Setup the base class that all of the SQL objects will inherit from.
#
# Note that the typing hint is required to be set at Any for mypy to be cool
# with it being a base class. This should be fixed with SQLAlchemy2.0
#
# For more context:
# https://docs.sqlalchemy.org/en/14/orm/extensions/mypy.html
# https://github.com/python/mypy/issues/2477


class DatabaseManager:
    """
    manager class for rds application operations
    """

    def __init__(
        self,
        experimental: bool = False,
        verbose: bool = False,
        seed: bool = False,
    ):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        if experimental:
            self.engine = get_experimental_engine(echo=verbose)
            # this is required for foreign key support in sqlite, per this:
            # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#foreign-key-support
            @sa.event.listens_for(sa.engine.Engine, "connect")
            def set_sqlite_pragma(dbapi_connection, _):  # type: ignore
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        else:
            self.engine = get_local_engine(echo=verbose)

        self.session = sessionmaker(bind=self.engine)

        # create tables in SqlBase
        SqlBase.metadata.create_all(self.engine)

        if seed:
            self.seed_metadata()

    def _get_schema_table(self, table: Any) -> sa.sql.schema.Table:
        if isinstance(table, sa.sql.schema.Table):
            return table
        if isinstance(table, sa.orm.decl_api.DeclarativeMeta):
            # mypy error: "DeclarativeMeta" has no attribute "__table__"
            return table.__table__  # type: ignore

        raise TypeError(f"can not pull schema table from {type(table)} type")

    def get_session(self) -> sessionmaker:
        """
        get db session for performing actions
        """
        return self.session

    def execute(
        self,
        statement: Union[
            sa.sql.selectable.Select,
            sa.sql.dml.Update,
            sa.sql.dml.Delete,
            sa.sql.dml.Insert,
        ],
    ) -> sa.engine.BaseCursorResult:
        """
        execute db action WITHOUT data
        """
        with self.session.begin() as cursor:
            result = cursor.execute(statement)
        return result  # type: ignore

    def execute_with_data(
        self,
        statement: Union[
            sa.sql.selectable.Select,
            sa.sql.dml.Update,
            sa.sql.dml.Delete,
            sa.sql.dml.Insert,
        ],
        data: pandas.DataFrame,
    ) -> sa.engine.BaseCursorResult:
        """
        execute db action WITH data as pandas dataframe
        """
        with self.session.begin() as cursor:
            result = cursor.execute(statement, data.to_dict(orient="records"))
        return result  # type: ignore

    def insert_dataframe(
        self, dataframe: pandas.DataFrame, insert_table: Any
    ) -> None:
        """
        insert data into db table from pandas dataframe
        """
        insert_as = self._get_schema_table(insert_table)

        with self.session.begin() as cursor:
            cursor.execute(
                sa.insert(insert_as),
                dataframe.to_dict(orient="records"),
            )

    def select_as_dataframe(
        self, select_query: sa.sql.selectable.Select
    ) -> pandas.DataFrame:
        """
        select data from db table and return pandas dataframe
        """
        with self.session.begin() as cursor:
            return pandas.DataFrame(
                [row._asdict() for row in cursor.execute(select_query)]
            )

    def select_as_list(
        self, select_query: sa.sql.selectable.Select
    ) -> List[Any]:
        """
        select data from db table and return list
        """
        with self.session.begin() as cursor:
            return [row._asdict() for row in cursor.execute(select_query)]

    def truncate_table(self, table_to_truncate: Any) -> None:
        """
        truncate db table

        sqlalchemy has no truncate operation so this is the closest equivalent
        """
        truncat_as = self._get_schema_table(table_to_truncate)

        truncat_as.drop(self.engine)
        truncat_as.create(self.engine)

    def seed_metadata(self) -> None:
        """
        seed metadata table for dev environment
        """
        process_logger = ProcessLogger("seed_metadata")
        process_logger.log_start()
        try:
            seed_file = os.path.join(
                HERE, "..", "tests", "july_17_filepaths.json"
            )
            with open(seed_file, "r", encoding="utf8") as seed_json:
                load_paths = json.load(seed_json)

            with self.session.begin() as session:
                session.execute(sa.insert(MetadataLog.__table__), load_paths)

            process_logger.log_complete()
        except Exception as exception:
            process_logger.log_failure(exception)


def get_unprocessed_files(
    path_contains: str, sql_session: sessionmaker
) -> Dict[str, Dict[str, List]]:
    """check metadata table for unprocessed parquet files"""
    process_logger = ProcessLogger(
        "get_unprocessed_files", seed_string=path_contains
    )
    process_logger.log_start()

    paths_to_load: Dict[str, Dict[str, List]] = {}
    try:
        read_md_log = sa.select((MetadataLog.pk_id, MetadataLog.path)).where(
            (MetadataLog.processed == sa.false())
            & (MetadataLog.path.contains(path_contains))
        )
        with sql_session.begin() as session:
            for path_id, path in session.execute(read_md_log):
                path = pathlib.Path(path)
                if path.parent not in paths_to_load:
                    paths_to_load[path.parent] = {"ids": [], "paths": []}
                paths_to_load[path.parent]["ids"].append(path_id)
                paths_to_load[path.parent]["paths"].append(str(path))

        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)

    return paths_to_load
