import logging
import os
import json
import pathlib
import urllib.parse
from typing import Dict, List, Any
import pandas

import sqlalchemy as sa
from sqlalchemy.engine import URL as DbUrl  # type: ignore
from sqlalchemy.orm import sessionmaker

from .postgres_schema import MetadataLog, SqlBase

HERE = os.path.dirname(os.path.abspath(__file__))


def get_local_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:  # type: ignore
    """
    Get an SQL Alchemy engine that connects to a locally Postgres RDS stood up
    via docker using env variables
    """
    try:
        host = os.environ["DB_HOST"]
        dbname = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        port = os.environ["DB_PORT"]
        password = urllib.parse.quote_plus(os.environ["DB_PASSWORD"])

        database_url = DbUrl.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=dbname,
        )
        logging.info("creating engine for %s", database_url.render_as_string())
        engine = sa.create_engine(database_url, echo=echo, future=True)
        return engine
    except Exception as exception:
        logging.error("Error Creating Sql Engine")
        logging.exception(exception)
        raise exception


def get_experimental_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:  # type: ignore
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


def get_aws_engine() -> sa.future.engine.Engine:  # type: ignore
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

    def __init__(self, experimental: bool = False, verbose: bool = False):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        if experimental:
            self.engine = get_experimental_engine(echo=verbose)

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

    def get_session(self) -> sessionmaker:
        """
        get db session for performing actions
        """
        return self.session

    def execute(self, statement: Any) -> None:
        """
        execute db action without data
        """
        with self.session.begin() as cursor:  # type: ignore
            cursor.execute(statement)

    def insert_dataframe(
        self, dataframe: pandas.DataFrame, insert_table: str
    ) -> None:
        """
        insert data into db table from pandas dataframe
        """
        with self.session.begin() as cursor:  # type: ignore
            cursor.execute(
                sa.insert(insert_table),
                dataframe.to_dict(orient="records"),
            )

    def select_as_dataframe(self, select_query: Any) -> pandas.DataFrame:
        """
        select data from db table and return pandas dataframe
        """
        with self.session.begin() as cursor:  # type: ignore
            return pandas.DataFrame(
                [row._asdict() for row in cursor.execute(select_query)]
            )

    def select_as_list(self, select_query: Any) -> List[Any]:
        """
        select data from db table and return list
        """
        with self.session.begin() as cursor:  # type: ignore
            return [row._asdict() for row in cursor.execute(select_query)]

    def truncate_table(self, table_to_truncate: Any) -> None:
        """
        truncate db table
        """
        table_to_truncate.__table__.drop(self.engine)
        table_to_truncate.__table__.create(self.engine)

    def seed_metadata(self) -> None:
        """
        seed metadata table for dev environment
        """
        try:
            seed_file = os.path.join(
                HERE, "..", "tests", "july_17_filepaths.json"
            )
            with open(seed_file, "r", encoding="utf8") as seed_json:
                load_paths = json.load(seed_json)

            with self.session.begin() as session:  # type: ignore
                session.execute(sa.insert(MetadataLog.__table__), load_paths)

        except Exception as e:
            logging.error("Error cleaning and seeding database")
            logging.exception(e)


def get_unprocessed_files(
    path_contains: str, sql_session: sessionmaker
) -> Dict[str, Dict[str, List]]:
    """check metadata table for unprocessed parquet files"""
    paths_to_load: Dict[str, Dict[str, List]] = {}
    try:
        read_md_log = sa.select((MetadataLog.pk_id, MetadataLog.path)).where(
            (MetadataLog.processed == sa.false())
            & (MetadataLog.path.contains(path_contains))
        )
        with sql_session.begin() as session:  # type: ignore
            for path_id, path in session.execute(read_md_log):
                path = pathlib.Path(path)
                if path.parent not in paths_to_load:
                    paths_to_load[path.parent] = {"ids": [], "paths": []}
                paths_to_load[path.parent]["ids"].append(path_id)
                paths_to_load[path.parent]["paths"].append(str(path))

    except Exception as e:
        logging.error("Error searching for unprocessed events")
        logging.exception(e)

    return paths_to_load
