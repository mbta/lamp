import logging
import os
import pathlib
import urllib.parse

from typing import Dict, List

import sqlalchemy as sa
from sqlalchemy.engine import URL as DbUrl  # type: ignore
from sqlalchemy.orm import sessionmaker

from .postgres_schema import MetadataLog


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


def write_from_dict(
    input_dictionary: dict,
    sql_session: sessionmaker,
    destination_table: sa.Table,
) -> None:
    """
    try to write a dict to a table. if experimental, use the testing engine,
    else use the local one.
    """
    try:
        with sql_session.begin() as session:  # type: ignore
            session.execute(sa.insert(destination_table), input_dictionary)
            session.commit()
    except Exception as e:
        logging.error("Error Writing Dataframe to Database")
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
