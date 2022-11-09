from typing import Dict, List, Any, Optional, Union, Tuple
import json
import os
import platform
import urllib.parse as urlparse

import boto3
import pandas

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from .logging_utils import ProcessLogger
from .postgres_schema import MetadataLog, SqlBase
from .s3_utils import get_utc_from_partition_path

HERE = os.path.dirname(os.path.abspath(__file__))


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


def postgres_event_create_modified_trigger(
    table: sa.schema.Table,
    connection: sa.engine.Connection,
    **_: Any,
) -> None:
    """
    sqlalchemy will listen for new table create events and when detected,
    run this function to suplement table creation by creating update_on
    triggers for tables containing an 'update_on' field

    this function will only be used for postgres db
    """
    update_column_name = "updated_on"
    trigger_function_statement = sa.DDL(
        f"CREATE OR REPLACE FUNCTION update_modified_columns() "
        f"RETURNS TRIGGER AS $$ "
        f"BEGIN "
        f"NEW.{update_column_name} = CURRENT_TIMESTAMP; "
        f"RETURN NEW; "
        f"END; $$ LANGUAGE plpgsql"
    )
    connection.execute(trigger_function_statement)
    if update_column_name in list(table.columns.keys()):
        trigger_name = f"update_{table}_modified".lower()
        trigger_statement = sa.DDL(
            f"DO $$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = '{trigger_name}') "
            f"THEN "
            f'    CREATE TRIGGER {trigger_name} BEFORE UPDATE ON "{table}" '
            f"    FOR EACH ROW EXECUTE PROCEDURE update_modified_columns();"
            f"END IF; "
            f"END $$;"
        )
        connection.execute(trigger_statement)


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
            db_password = get_db_password()

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
        verbose: bool = False,
        seed: bool = False,
    ):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        self.engine = get_local_engine(echo=verbose)

        sa.event.listen(
            sa.schema.Table,
            "after_create",
            postgres_event_create_modified_trigger,
        )

        sa.event.listen(
            self.engine,
            "do_connect",
            postgres_event_update_db_password,
        )

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
    ) -> Union[List[Any], List[Dict[str, Any]]]:
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
    path_contains: str,
    db_manager: DatabaseManager,
    file_limit: Optional[int] = None,
) -> List[Dict[str, List]]:
    """
    check metadata table for unprocessed parquet files
    groups files into batches of similar partition paths
    sorts partition paths from oldest to most recent

    returns sorted list of path dictionaries with following layout:
    {
        "ids": [metadata table ids],
        "paths": [s3 paths of parquet files that share path]
    }
    """
    process_logger = ProcessLogger(
        "get_unprocessed_files", seed_string=path_contains
    )
    process_logger.log_start()

    paths_to_load: Dict[float, Dict[str, List]] = {}
    try:
        read_md_log = sa.select((MetadataLog.pk_id, MetadataLog.path)).where(
            (MetadataLog.processed == sa.false())
            & (MetadataLog.path.contains(path_contains))
        )
        for path_record in db_manager.select_as_list(read_md_log):
            path_id = path_record.get("pk_id")
            path = str(path_record.get("path"))
            path_timestamp = get_utc_from_partition_path(path)

            if path_timestamp not in paths_to_load:
                paths_to_load[path_timestamp] = {"ids": [], "paths": []}
            paths_to_load[path_timestamp]["ids"].append(path_id)
            paths_to_load[path_timestamp]["paths"].append(path)

        paths_found = len(paths_to_load)
        paths_returned = paths_found
        if file_limit is not None:
            paths_returned = file_limit

        process_logger.add_metadata(
            paths_found=paths_found, paths_returned=paths_returned
        )

        process_logger.log_complete()

    except Exception as exception:
        process_logger.log_failure(exception)

    return [
        paths_to_load[timestamp] for timestamp in sorted(paths_to_load.keys())
    ][:file_limit]
