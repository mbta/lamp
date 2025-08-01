import os
from abc import ABC
from abc import abstractmethod
from itertools import chain
from typing import Dict
from typing import Optional

import pyarrow
from pyarrow import fs
import pyarrow.parquet as pq
from tableauhyperapi import (
    TableDefinition,
    SqlType,
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    escape_string_literal,
)

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import (
    download_file,
    upload_file,
    object_metadata,
)

from .server import (
    overwrite_datasource,
    datasource_from_name,
)


class HyperJob(ABC):  # pylint: disable=R0902
    """
    Abstract Base Class for Parquet / Tableau HyperFile jobs
    """

    def __init__(
        self,
        hyper_file_name: str,
        remote_parquet_path: str,
        lamp_version: str,
        project_name: str = os.getenv("TABLEAU_PROJECT", ""),
    ) -> None:

        # extracts one of "dev", "staging", or "prod" from the ECS_TASK_GROUP variable
        environment = os.getenv("ECS_TASK_GROUP", "-").split("-")[-1]
        if environment != "prod":
            hyper_file_name = f"{hyper_file_name.replace('.hyper','')}_{environment}.hyper"
        self.hyper_file_name = hyper_file_name

        self.hyper_table_name = hyper_file_name.replace(".hyper", "")
        self.remote_parquet_path = remote_parquet_path
        self.lamp_version = lamp_version
        self.project_name = project_name
        self.local_parquet_path = "/tmp/local.parquet"
        self.local_hyper_path = f"/tmp/{hyper_file_name}"
        self.remote_fs = fs.LocalFileSystem()
        if remote_parquet_path.startswith("s3://"):
            self.remote_fs = fs.S3FileSystem()
            self.remote_parquet_path = self.remote_parquet_path.replace("s3://", "")

    @property
    @abstractmethod
    def output_processed_schema(self) -> pyarrow.schema:
        """
        Schema for Job parquet file
        """

    @abstractmethod
    def create_parquet(self, db_manager: Optional[DatabaseManager]) -> None:
        """
        Business logic to create new Job parquet file
        """

    @abstractmethod
    def update_parquet(self, db_manager: Optional[DatabaseManager]) -> bool:
        """
        Business logic to update existing Job parquet file

        :return: True if local parquet file was updated, else False
        """

    def convert_parquet_dtype(self, dtype: pyarrow.DataType) -> SqlType:
        """
        Converts pyarrow.DataType into HyperFile SqlType
        for creation of HyperFile TableDefinition object

        Tableau HyperFiles do not support unsigned integers

        SqlType.smallint(): 2 byte signed i16
        SqlType.int(): 4 byte signed i32
        SqlType.bigint(): 8 byte signed i64
        """
        dtype = str(dtype)
        dtype_map = {
            "int8": SqlType.small_int(),
            "uint8": SqlType.small_int(),
            "int16": SqlType.small_int(),
            "uint16": SqlType.int(),
            "int32": SqlType.int(),
            "uint32": SqlType.big_int(),
            "int64": SqlType.big_int(),
            "bool": SqlType.bool(),
            "halffloat": SqlType.double(),
            "float": SqlType.double(),
            "double": SqlType.double(),
        }

        map_check = dtype_map.get(dtype)
        if map_check is not None:
            return map_check

        if dtype.startswith("date32"):
            return SqlType.date()

        if dtype.startswith("timestamp"):
            return SqlType.timestamp()

        return SqlType.text()

    def max_stats_of_parquet(self) -> Dict[str, str]:
        """
        Create dictionary of maximum value for each column of locally saved
        parquet file

        :return Dict[column_name: max_column_value]
        """
        # get row_groups from parquet metadata (list of dicts)
        row_groups = pq.read_metadata(self.local_parquet_path).to_dict()["row_groups"]

        # explode columns element from all row  groups into flat list
        parquet_column_stats = list(chain.from_iterable([row_group["columns"] for row_group in row_groups]))

        return {
            col["path_in_schema"]: col["statistics"].get("max") for col in parquet_column_stats if col["statistics"]
        }

    def remote_version_match(self) -> bool:
        """
        Compare "lamp_version" of remote parquet file to expected.

        :return True if remote and expected version match, else False
        """
        lamp_version = object_metadata(self.remote_parquet_path).get("lamp_version", "")

        return lamp_version == self.lamp_version

    def create_local_hyper(self, use_local=False) -> int:
        """
        Create local hyper file, from remote parquet file
        """

        if use_local is not True:
            if os.path.exists(self.local_hyper_path):
                os.remove(self.local_hyper_path)

        # create HyperFile TableDefinition based on Job parquet schema
        hyper_table_schema = TableDefinition(
            table_name=self.hyper_table_name,
            columns=[
                TableDefinition.Column(col.name, self.convert_parquet_dtype(col.type))
                for col in self.output_processed_schema
            ],
        )
        if use_local is not True:

            download_file(
                object_path=self.remote_parquet_path,
                file_name=self.local_parquet_path,
            )

        # create local HyperFile based on remote parquet file
        # log_config = "" disables creation of local logfile
        with HyperProcess(
            telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
            parameters={"log_config": ""},
        ) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=self.local_hyper_path,
                create_mode=CreateMode.CREATE_AND_REPLACE,
            ) as connect:
                connect.catalog.create_table(table_definition=hyper_table_schema)
                copy_command = (
                    f"COPY {hyper_table_schema.table_name} "
                    f"FROM {escape_string_literal(self.local_parquet_path)} "
                    "WITH (FORMAT PARQUET)"
                )

                count_inserted = connect.execute_command(copy_command)

        os.remove(self.local_parquet_path)

        return count_inserted

    def run_hyper(self) -> None:
        """
        Update Tableau HyperFile if remote parquet file was modified
        """
        max_retries = 3
        process_log = ProcessLogger(
            "hyper_job_run_hyper",
            hyper_file_name=self.hyper_file_name,
            tableau_project=self.project_name,
            max_retries=max_retries,
        )
        process_log.log_start()

        for retry_count in range(max_retries + 1):
            try:
                process_log.add_metadata(retry_count=retry_count)
                # get datasource from Tableau to check "updated_at" datetime
                datasource = datasource_from_name(self.hyper_table_name, self.project_name)

                # get file_info on remote parquet file to check "mtime" datetime
                pq_file_info = self.remote_fs.get_file_info(self.remote_parquet_path)

                # Parquet file does not exist, can not run upload
                if pq_file_info.type == fs.FileType.NotFound:
                    raise FileNotFoundError(f"{self.remote_parquet_path} does not exist")

                process_log.add_metadata(
                    parquet_last_mod=pq_file_info.mtime.isoformat(),
                )

                if datasource and datasource.updated_at:
                    process_log.add_metadata(
                        hyper_last_mod=datasource.updated_at.isoformat(),
                    )

                # if datasource exists and parquet file was not modified, skip HyperFile update
                if datasource is not None and pq_file_info.mtime < datasource.updated_at:
                    process_log.add_metadata(update_hyper_file=False)
                    process_log.log_complete()
                    break

                hyper_row_count = self.create_local_hyper()
                hyper_file_size = os.path.getsize(self.local_hyper_path) / (1024 * 1024)
                process_log.add_metadata(
                    hyper_row_count=hyper_row_count,
                    hyper_file_siz_mb=f"{hyper_file_size:.2f}",
                    update_hyper_file=True,
                )

                # Upload local HyperFile to Tableau server
                overwrite_datasource(
                    project_name=self.project_name,
                    hyper_path=self.local_hyper_path,
                )
                os.remove(self.local_hyper_path)

                process_log.log_complete()

                break

            except Exception as exception:
                if retry_count == max_retries:
                    process_log.log_failure(exception=exception)

    def run_parquet(self, db_manager: Optional[DatabaseManager]) -> None:
        """
        Remote parquet Create / Update runner

        If Remote parquet file does not exist, or Job parquet schema does not
        match remote parquet schema, create new remote parquet file.

        Otherwise, update existing remote parquet file
        """
        process_log = ProcessLogger(
            "hyper_job_run_parquet",
            remote_path=self.remote_parquet_path,
        )
        process_log.log_start()

        try:
            remote_schema_match = False
            remote_version_match = False
            file_info = self.remote_fs.get_file_info(self.remote_parquet_path)

            if file_info.type == fs.FileType.File:
                # get remote parquet schema and compare to expected local schema
                remote_schema = pq.read_schema(
                    self.remote_parquet_path,
                    filesystem=self.remote_fs,
                )
                remote_schema_match = self.output_processed_schema.equals(remote_schema)
                remote_version_match = self.remote_version_match()

            if remote_schema_match is False or remote_version_match is False:
                # create new parquet if no remote parquet found or
                # remote schema does not match expected local schema
                run_action = "create"
                upload_parquet = True
                self.create_parquet(db_manager)
            else:
                run_action = "update"
                upload_parquet = self.update_parquet(db_manager)

            parquet_file_size_mb = 0.0
            if os.path.exists(self.local_parquet_path):
                parquet_file_size_mb = os.path.getsize(self.local_parquet_path) / (1024 * 1024)

            process_log.add_metadata(
                remote_schema_match=remote_schema_match,
                run_action=run_action,
                upload_parquet=upload_parquet,
                parquet_file_size_mb=f"{parquet_file_size_mb:.2f}",
            )

            if upload_parquet:
                upload_file(
                    file_name=self.local_parquet_path,
                    object_path=self.remote_parquet_path,
                    extra_args={"Metadata": {"lamp_version": self.lamp_version}},
                )

            if os.path.exists(self.local_parquet_path):
                os.remove(self.local_parquet_path)

            process_log.log_complete()

        except Exception as exception:
            process_log.log_failure(exception=exception)

    def run_parquet_hyper_combined_job(self, db_manager: DatabaseManager | None = None) -> None:
        """
        Remote parquet Create / Update runner / Hyper uploader
        """
        process_log_create = ProcessLogger(
            "hyper_job_run_parquet_one_shot",
            remote_path=self.remote_parquet_path,
        )
        process_log_create.log_start()

        try:
            self.create_parquet(db_manager)
            process_log_create.log_complete()
        except Exception as exception:
            process_log_create.log_failure(exception=exception)

        # Update Tableau HyperFile
        max_retries = 3
        process_log = ProcessLogger(
            "hyper_job_run_hyper_one_shot",
            hyper_file_name=self.hyper_file_name,
            tableau_project=self.project_name,
            max_retries=max_retries,
        )
        process_log.log_start()

        for retry_count in range(max_retries + 1):
            try:
                process_log.add_metadata(retry_count=retry_count)
                # get datasource from Tableau to check "updated_at" datetime

                # create_local_hyper removes the parquet file and leaves just hyper
                hyper_row_count = self.create_local_hyper(use_local=True)
                hyper_file_size = os.path.getsize(self.local_hyper_path) / (1024 * 1024)
                process_log.add_metadata(
                    hyper_row_count=hyper_row_count,
                    hyper_file_siz_mb=f"{hyper_file_size:.2f}",
                    update_hyper_file=True,
                )

                # Upload local HyperFile to Tableau server
                overwrite_datasource(
                    project_name=self.project_name,
                    hyper_path=self.local_hyper_path,
                )
                os.remove(self.local_hyper_path)

                process_log.log_complete()

                break

            except Exception as exception:
                if retry_count == max_retries:
                    process_log.log_failure(exception=exception)
