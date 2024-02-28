import os
import time
import urllib.parse as urlparse
from enum import Enum, auto
from queue import Queue
from multiprocessing import Manager, Process
from typing import Any, Dict, List, Optional, Tuple, Union, Callable

import boto3
import pandas
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import pyarrow
import pyarrow.parquet as pq

from lamp_py.aws.s3 import get_datetime_from_partition_path
from lamp_py.runtime_utils.process_logger import ProcessLogger

from .metadata_schema import MetadataLog
from .rail_performance_manager_schema import VehicleTrips


def running_in_docker() -> bool:
    """
    return true if running inside of a docker container
    """
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")
        or os.path.isfile(path)
        and any("docker" in line for line in open(path, encoding="UTF-8"))
    )


def running_in_aws() -> bool:
    """
    return True if running on aws, else False
    """
    return bool(os.getenv("AWS_DEFAULT_REGION"))


def environ_get(var_name: str) -> str:
    """
    get an environment variable, raising an error if it does not exist. this
    utility helps with type checking.
    """
    value = os.environ.get(var_name)
    if value is None:
        raise KeyError(f"Unable to find {var_name} in environment")
    return value


class PsqlArgs:
    """
    container class for arguments needed to log into postgres db
    """

    def __init__(self, prefix: str):
        self.host: str
        if not running_in_docker() and not running_in_aws():
            # running on the command line. use localhost ip
            self.host = "127.0.0.1"
        else:
            # running in docker, use the env variable pointing to the image
            #   name in the container.
            # OR
            # running on aws, use the env variable resolving to the aws rds
            #   instance
            self.host = environ_get(f"{prefix}_DB_HOST")

        self.port: str
        if running_in_docker():
            # running in docker, use the default port for postgres
            self.port = "5432"
        else:
            # running on the command line, use the forwarded port out of the
            #   container
            # OR
            # running on aws, use the env var for the the aws rds instance
            self.port = environ_get(f"{prefix}_DB_PORT")

        self.name: str = environ_get(f"{prefix}_DB_NAME")
        self.user: str = environ_get(f"{prefix}_DB_USER")
        self.password: Optional[str] = os.environ.get(f"{prefix}_DB_PASSWORD")

    def get_password(self) -> str:
        """
        function to provide rds password

        used to refresh auth token, if required
        """
        if self.password is not None:
            return self.password

        region = os.environ.get("DB_REGION", None)

        # generate ws db auth token if in rds
        client = boto3.client("rds")
        return client.generate_db_auth_token(
            DBHostname=self.host,
            Port=self.port,
            DBUsername=self.user,
            Region=region,
        )

    def get_local_engine(
        self,
        echo: bool = False,
    ) -> sa.future.engine.Engine:
        """
        Get an QL Alchemy engine that connects to a locally Postgres RDS stood
        via docker using env variables
        """
        process_logger = ProcessLogger("create_sql_engine")
        process_logger.log_start()
        try:
            process_logger.add_metadata(**self.metadata())

            # use presence of password as indicator of connection type.
            #
            # if its not provided, assume cloud database where ssl is used and
            # passwords are generated on the fly
            #
            # if it is provided, assume local docker database
            db_ssl_options = ""
            db_password = self.password
            if db_password is None:
                db_password = self.get_password()
                db_password = urlparse.quote_plus(db_password)

                assert db_password is not None
                assert db_password != ""

                # set the ssl cert path to the file that should be added to the
                # lambda function at deploy time
                db_ssl_cert = os.path.abspath(
                    os.path.join(
                        "/", "usr", "local", "share", "amazon-certs.pem"
                    )
                )

                assert os.path.isfile(db_ssl_cert)

                # update the ssl options string to add to the database url
                db_ssl_options = (
                    f"?sslmode=verify-full&sslrootcert={db_ssl_cert}"
                )

            database_url = (
                f"postgresql+psycopg2://{self.user}:"
                f"{db_password}@{self.host}:{self.port}/{self.name}"
                f"{db_ssl_options}"
            )

            print(database_url)

            engine = sa.create_engine(
                database_url,
                echo=echo,
                future=True,
                pool_pre_ping=True,
                pool_use_lifo=True,
                pool_size=5,
                max_overflow=2,
                connect_args={
                    "keepalives": 1,
                    "keepalives_idle": 60,
                    "keepalives_interval": 60,
                },
            )

            process_logger.log_complete()
            return engine
        except Exception as exception:
            process_logger.log_failure(exception)
            raise exception

    def metadata(self) -> Dict[str, str]:
        """
        generate a dict to add to logs for psql connection details
        """
        return {
            "host": self.host,
            "database_name": self.name,
            "user": self.user,
            "port": self.port,
        }


class DatabaseIndex(Enum):
    """
    enum for different databases the projects can use
    """

    METADATA = auto()
    RAIL_PERFORMANCE_MANAGER = auto()

    def get_env_prefix(self) -> str:
        """
        in the environment, all keys for this database have this prefix
        """
        if self == DatabaseIndex.RAIL_PERFORMANCE_MANAGER:
            return "RPM"
        if self == DatabaseIndex.METADATA:
            return "MD"
        raise NotImplementedError("No environment prefix for index {self.name}")

    def get_args_from_env(self) -> PsqlArgs:
        """
        generate a sql argument instance for this ind
        """
        prefix = self.get_env_prefix()
        return PsqlArgs(prefix)


def generate_update_db_password_func(psql_args: PsqlArgs) -> Callable:
    """
    create a function to update the password for a database when a new
    connection is created
    """

    def postgres_event_update_db_password(
        _: sa.engine.interfaces.Dialect,
        __: Any,
        ___: Tuple[Any, ...],
        cparams: Dict[str, Any],
    ) -> None:
        """
        update database password on every new connection attempt
        this will refresh db auth token passwords
        """
        process_logger = ProcessLogger("password_refresh")
        process_logger.log_start()
        cparams["password"] = psql_args.get_password()
        process_logger.log_complete()

    return postgres_event_update_db_password


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

    def __init__(self, db_index: DatabaseIndex, verbose: bool = False):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        self.db_index = db_index
        self.psql_args = db_index.get_args_from_env()
        self.engine = self.psql_args.get_local_engine(echo=verbose)

        sa.event.listen(
            self.engine,
            "do_connect",
            generate_update_db_password_func(self.psql_args),
        )

        self.session = sessionmaker(bind=self.engine)

    def _get_schema_table(self, table: Any) -> sa.sql.schema.Table:
        if isinstance(table, sa.sql.schema.Table):
            return table
        if isinstance(
            table,
            (
                sa.orm.decl_api.DeclarativeMeta,
                sa.orm.decl_api.DeclarativeAttributeIntercept,
            ),
        ):
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
            sa.sql.elements.TextClause,
        ],
        disable_trip_tigger: bool = False,
    ) -> sa.engine.CursorResult:
        """
        execute db action WITHOUT data

        :param disable_trip_trigger if True, will disable rt_trips_update_branch_trunk TRIGGER on vehicle_trips table
        """
        if disable_trip_tigger:
            self._disable_trip_trigger()

        with self.session.begin() as cursor:
            result = cursor.execute(statement)

        if disable_trip_tigger:
            self._enable_trip_trigger()

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
        disable_trip_tigger: bool = False,
    ) -> sa.engine.CursorResult:
        """
        execute db action WITH data as pandas dataframe

        :param disable_trip_trigger if True, will disable rt_trips_update_branch_trunk TRIGGER on vehicle_trips table
        """
        if disable_trip_tigger:
            self._disable_trip_trigger()

        with self.session.begin() as cursor:
            result = cursor.execute(statement, data.to_dict(orient="records"))

        if disable_trip_tigger:
            self._enable_trip_trigger()

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

    def write_to_parquet(
        self,
        select_query: sa.sql.selectable.Select,
        write_path: str,
        schema: pyarrow.schema,
        batch_size: int = 1024 * 1024,
    ) -> None:
        """
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
            "postgres_write_to_parquet",
            batch_size=batch_size,
            write_path=write_path,
        )
        process_logger.log_start()

        part_stmt = select_query.execution_options(
            stream_results=True,
            max_row_buffer=batch_size,
        )
        with self.session.begin() as cursor:
            with pq.ParquetWriter(write_path, schema=schema) as pq_writer:
                for part in cursor.execute(part_stmt).partitions(batch_size):
                    pq_writer.write_batch(
                        pyarrow.RecordBatch.from_pylist(
                            [row._asdict() for row in part], schema=schema
                        )
                    )

        process_logger.log_complete()

    def truncate_table(
        self,
        table_to_truncate: Any,
        restart_identity: bool = False,
        cascade: bool = False,
    ) -> None:
        """
        truncate db table

        restart_identity: Automatically restart sequences owned by columns of the truncated table(s).
        cascade: Automatically truncate all tables that have foreign-key references to any of the named tables, or to any tables added to the group due to CASCADE.
        """
        truncat_as = self._get_schema_table(table_to_truncate)

        truncate_query = f"TRUNCATE {truncat_as}"

        if restart_identity:
            truncate_query = f"{truncate_query} RESTART IDENTITY"

        if cascade:
            truncate_query = f"{truncate_query} CASCADE"

        self.execute(sa.text(f"{truncate_query};"))

        # Execute VACUUM to avoid non-deterministic behavior during testing
        self.vacuum_analyze(table_to_truncate)

    def vacuum_analyze(self, table: Any) -> None:
        """RUN VACUUM (ANALYZE) on table"""
        table_as = self._get_schema_table(table)

        with self.session.begin() as cursor:
            cursor.execute(sa.text("END TRANSACTION;"))
            cursor.execute(sa.text(f"VACUUM (ANALYZE) {table_as};"))

    def _disable_trip_trigger(self) -> None:
        """
        DISABLE rt_trips_update_branch_trunk TRIGGER on vehicle_trips table

        Based on our current RDS schema, any UPDATE action against the vehicle_trips
        table results in the rt_trips_update_branch_trunk TRIGGER running an expensive
        evaluation against all UPDATED records to determine the trunk_route_id and branch_route_id
        for the trip. This evaluation is rarely needed, and is slowlying down very
        simple queries.
        """
        table = self._get_schema_table(VehicleTrips)
        disable_trigger = (
            f"ALTER TABLE {table} DISABLE TRIGGER rt_trips_update_branch_trunk;"
        )

        with self.session.begin() as cursor:
            cursor.execute(sa.text(disable_trigger))

    def _enable_trip_trigger(self) -> None:
        """
        ENABLE rt_trips_update_branch_trunk TRIGGER on vehicle_trips table
        """
        table = self._get_schema_table(VehicleTrips)
        enable_trigger = (
            f"ALTER TABLE {table} ENABLE TRIGGER rt_trips_update_branch_trunk;"
        )

        with self.session.begin() as cursor:
            cursor.execute(sa.text(enable_trigger))


def seed_metadata(md_db_manager: DatabaseManager, paths: List[str]) -> None:
    """
    add metadata filepaths to metadata table for testing
    """
    with md_db_manager.session.begin() as session:
        session.execute(
            sa.insert(MetadataLog.__table__),
            [{"path": p} for p in paths],
        )


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
        read_md_log = sa.select(MetadataLog.pk_id, MetadataLog.path).where(
            (MetadataLog.rail_pm_processed == sa.false())
            & (MetadataLog.path.contains(path_contains))
        )
        for path_record in db_manager.select_as_list(read_md_log):
            path_id = path_record.get("pk_id")
            path = str(path_record.get("path"))
            path_timestamp = get_datetime_from_partition_path(path).timestamp()

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


def _rds_writer_process(metadata_queue: Queue[Optional[str]]) -> None:
    """
    process for writing matadata paths recieved from metadata_queue

    if None recieved from queue, process will exit
    """
    process_logger = ProcessLogger("rds_writer_process")
    process_logger.log_start()

    db_index = DatabaseIndex.METADATA
    psql_args = db_index.get_args_from_env()
    engine = psql_args.get_local_engine()

    sa.event.listen(
        engine,
        "do_connect",
        generate_update_db_password_func(psql_args),
    )

    while True:
        metadata_path = metadata_queue.get()

        if metadata_path is None:
            break

        insert_statement = sa.insert(MetadataLog.__table__).values(
            path=metadata_path
        )
        insert_logger = ProcessLogger("metadata_insert", filepath=metadata_path)
        insert_logger.log_start()
        retry_attempt = 0

        # All metatdata_insert attempts must succeed, keep attempting until success
        while True:
            try:
                with engine.begin() as cursor:
                    cursor.execute(insert_statement)

            except Exception as _:
                retry_attempt += 1
                # wait for gremlins to disappear
                time.sleep(15)

            else:
                insert_logger.add_metadata(retry_attempts=retry_attempt)
                insert_logger.log_complete()
                break

    process_logger.log_complete()


def start_rds_writer_process() -> Tuple[Queue[Optional[str]], Process]:
    """
    create metadata queue and rds writer process

    return metadata queue
    """
    queue_manager = Manager()
    metadata_queue: Queue[Optional[str]] = queue_manager.Queue()

    writer_process = Process(target=_rds_writer_process, args=(metadata_queue,))
    writer_process.start()

    return (metadata_queue, writer_process)
