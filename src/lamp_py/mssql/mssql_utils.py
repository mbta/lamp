import os
from typing import Any, Dict, List, Union

import pyodbc
import pandas
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import pyarrow
import pyarrow.parquet as pq

from lamp_py.runtime_utils.process_logger import ProcessLogger


def get_local_engine(echo: bool = False) -> sa.future.engine.Engine:
    """
    Get an SQL Alchemy engine that connects to a MSSQL RDS
    """
    process_logger = ProcessLogger("create_mssql_engine")
    process_logger.log_start()
    try:
        # disable pyodbc pooling because sqlachemy does pooling
        pyodbc.pooling = False

        db_host = os.environ.get("TM_DB_HOST")
        db_name = os.environ.get("TM_DB_NAME")
        db_user = os.environ.get("TM_DB_USER")
        db_password = os.environ.get("TM_DB_PASSWORD")
        db_port = int(os.environ.get("TM_DB_PORT", 0))

        assert db_host is not None
        assert db_name is not None
        assert db_user is not None
        assert db_password is not None
        assert db_port != 0

        process_logger.add_metadata(
            host=db_host, database_name=db_name, user=db_user, port=db_port
        )

        connection = sa.URL.create(
            "mssql+pyodbc",
            username=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name,
        )

        engine = sa.create_engine(
            connection,
            connect_args={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
                "Encrypt": "no",
            },
            echo=echo,
        )

        process_logger.log_complete()
        return engine
    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception


class MSSQLManager:
    """
    manager class for rds application operations
    """

    def __init__(self, verbose: bool = False):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        self.engine = get_local_engine(echo=verbose)

        self.session = sessionmaker(bind=self.engine)

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
            sa.sql.elements.TextClause,
        ],
    ) -> sa.engine.Result:
        """
        execute db action WITHOUT data
        """
        with self.session.begin() as cursor:
            result = cursor.execute(statement)

        return result

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

    def write_to_parquet(
        self,
        select_query: sa.sql.selectable.Select,
        write_path: str,
        schema: pyarrow.schema,
        batch_size: int = 1024 * 1024,
    ) -> None:
        """
        WARNING!!! The streaming methods used here may or may not be implemented
        for the MSSQL driver being used by the database engine. Testing is required to
        confirm resource usage and behavior for very large queries

        stream db query results to parquet file in batches

        this function is meant to limit memory usage when creating very large
        parquet files from db SELECT

        default batch_size of 1024*1024 is based on "row_group_size" parameter
        of ParquetWriter.write_batch(): row group size will be the minimum of
        the RecordBatch size and 1024 * 1024. If set larger
        than 64Mi then 64Mi will be used instead.
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html#pyarrow.parquet.ParquetWriter.write_batch

        :param select_query: query to execute
        :param write_path: local file path for resulting parquet file
        :param schema: schema of parquet file from select query
        :param batch_size: number of records to stream from db per batch
        """
        process_logger = ProcessLogger(
            "mssql_write_to_parquet",
            batch_size=batch_size,
            write_path=write_path,
        )
        process_logger.log_start()

        part_stmt = select_query.execution_options(
            stream_results=True,
            max_row_buffer=batch_size,
        )
        max_retries = 3
        for retry_attempts in range(max_retries + 1):
            process_logger.add_metadata(retry_attempts=retry_attempts)
            try:
                with self.session.begin() as cursor:
                    with pq.ParquetWriter(
                        write_path, schema=schema
                    ) as pq_writer:
                        for part in cursor.execute(part_stmt).partitions(
                            batch_size
                        ):
                            pq_writer.write_batch(
                                pyarrow.RecordBatch.from_pylist(
                                    [row._asdict() for row in part],
                                    schema=schema,
                                )
                            )

                process_logger.log_complete()

            except Exception as exception:
                if retry_attempts == max_retries:
                    process_logger.log_failure(exception)
