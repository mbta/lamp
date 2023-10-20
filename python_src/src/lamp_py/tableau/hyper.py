import os
from abc import ABC
from abc import abstractmethod
from itertools import chain
from typing import Dict

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
        project_name: str = "Staging Data",
    ) -> None:
        self.hyper_file_name = hyper_file_name
        self.hyper_table_name = hyper_file_name.replace(".hyper", "")
        self.remote_parquet_path = remote_parquet_path
        self.project_name = project_name
        self.local_parquet_path = "/tmp/local.parquet"
        self.local_hyper_path = f"/tmp/{hyper_file_name}"

        self.remote_fs = fs.LocalFileSystem()
        if remote_parquet_path.startswith("s3://"):
            self.remote_fs = fs.S3FileSystem()
            self.remote_parquet_path = self.remote_parquet_path.replace(
                "s3://", ""
            )

        self.db_manager = DatabaseManager()

    @property
    @abstractmethod
    def parquet_schema(self) -> pyarrow.schema:
        """
        Schema for Job parquet file
        """

    @abstractmethod
    def create_parquet(self) -> None:
        """
        Business logic to create new Job parquet file
        """

    @abstractmethod
    def update_parquet(self) -> bool:
        """
        Business logic to update existing Job parquet file

        :return bool (parquet was updated)
        """

    def convert_parquet_dtype(self, dtype: pyarrow.DataType) -> SqlType:
        """
        SqlType.smallint(): 2 byte signed i16
        SqlType.int(): 4 byte signed i32
        SqlType.bigint(): 8 byte signed i64
        """
        dtype = str(dtype)
        dtype_map = {
            "int8": SqlType.small_int(),
            "int16": SqlType.small_int(),
            "int32": SqlType.int(),
            "int64": SqlType.big_int(),
            "bool": SqlType.bool(),
            "float16": SqlType.double(),
            "float32": SqlType.double(),
            "float64": SqlType.double(),
        }

        map_check = dtype_map.get(dtype)
        if map_check is not None:
            return map_check

        if dtype.startswith("date32"):
            return SqlType.date()

        if dtype.startswith("timestamp"):
            return SqlType.timestamp()

        return SqlType.text()

    def upload_parquet(self) -> None:
        """
        Upload local parquet file to remote path
        """
        process_logger = ProcessLogger(
            "hyper_job_upload_parquet",
            source=self.local_parquet_path,
            destination=self.remote_parquet_path,
        )
        process_logger.log_start()
        fs.copy_files(
            source=self.local_parquet_path,
            destination=self.remote_parquet_path,
            source_filesystem=fs.LocalFileSystem(),
            destination_filesystem=self.remote_fs,
        )
        process_logger.log_complete()

    def download_parquet(self) -> None:
        """
        Download remote parquet file to local path
        """
        process_logger = ProcessLogger(
            "hyper_job_download_parquet",
            source=self.remote_parquet_path,
            destination=self.local_parquet_path,
        )
        process_logger.log_start()

        if os.path.exists(self.local_parquet_path):
            os.remove(self.local_parquet_path)

        fs.copy_files(
            source=self.remote_parquet_path,
            destination=self.local_parquet_path,
            source_filesystem=self.remote_fs,
            destination_filesystem=fs.LocalFileSystem(),
        )

        process_logger.log_complete()

    def max_stats_of_parquet(self) -> Dict[str, str]:
        """
        Create dictionary of maximum value for each column of locally saved
        parquet file
        """
        # get row_groups from parquet metadata (list of dicts)
        row_groups = pq.read_metadata(self.local_parquet_path).to_dict()[
            "row_groups"
        ]

        # explode columns element from all row  groups into flat list
        parquet_column_stats = list(
            chain.from_iterable(
                [row_group["columns"] for row_group in row_groups]
            )
        )

        return {
            col["path_in_schema"]: col["statistics"].get("max")
            for col in parquet_column_stats
            if col["statistics"]
        }

    def create_local_hyper(self) -> int:
        """
        Create local hyper file, from remote parquet file
        """
        if os.path.exists(self.local_hyper_path):
            os.remove(self.local_hyper_path)

        # create HyperFile TableDefinition based on Job parquet schema
        hyper_table_schema = TableDefinition(
            table_name=self.hyper_table_name,
            columns=[
                TableDefinition.Column(
                    col.name, self.convert_parquet_dtype(col.type)
                )
                for col in self.parquet_schema
            ],
        )

        self.download_parquet()

        # create local HyperFile based on remote parquet file
        with HyperProcess(
            telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
        ) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                database=self.local_hyper_path,
                create_mode=CreateMode.CREATE_AND_REPLACE,
            ) as connect:
                connect.catalog.create_table(
                    table_definition=hyper_table_schema
                )
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
        max_retries = 5
        process_log = ProcessLogger(
            "hyper_job_run_hyper",
            hyper_file_name=self.hyper_file_name,
            tableau_project=self.project_name,
        )
        process_log.log_start()

        for retry_count in range(max_retries):
            try:
                process_log.add_metadata(retry_count=retry_count)
                # get datasource from Tableau to check "updated_at" datetime
                datasource = datasource_from_name(self.hyper_table_name)

                # get file_info on remote parquet file to check "mtime" datetime
                pq_file_info = self.remote_fs.get_file_info(
                    self.remote_parquet_path
                )

                # Parquet file does not exist, can not run upload
                if pq_file_info.type == fs.FileType.NotFound:
                    process_log.add_metadata(no_remote_parquet=True)
                    break

                process_log.add_metadata(
                    parquet_last_mod=pq_file_info.mtime.isoformat(),
                )

                if datasource and datasource.updated_at:
                    process_log.add_metadata(
                        hyper_last_mod=datasource.updated_at.isoformat(),
                    )

                # if datasource exists and parquet file was not modified, skip HyperFile update
                if (
                    datasource is not None
                    and pq_file_info.mtime < datasource.updated_at
                ):
                    process_log.add_metadata(update_hyper_file=False)
                    break

                hyper_row_count = self.create_local_hyper()
                hyper_file_size = os.path.getsize(self.local_hyper_path) / (
                    1024 * 1024
                )
                process_log.add_metadata(
                    hyper_row_count=hyper_row_count,
                    hyper_file_size=f"{hyper_file_size:.2f}_MB",
                    update_hyper_file=True,
                )

                # Upload local HyperFile to Tableau server
                overwrite_datasource(
                    project_name=self.project_name,
                    hyper_path=self.local_hyper_path,
                )
                os.remove(self.local_hyper_path)

            except Exception as e:
                if retry_count == max_retries - 1:
                    process_log.log_failure(exception=e)
                    return

            else:
                break

        process_log.log_complete()

    def run_parquet(self) -> None:
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
            file_info = self.remote_fs.get_file_info(self.remote_parquet_path)
            process_log.add_metadata(remote_file_type=file_info.type)

            # get remote parquet schema to compare to expected local schema
            if file_info.type == fs.FileType.File:
                remote_schema = pq.read_schema(
                    self.remote_parquet_path, filesystem=self.remote_fs
                )
                process_log.add_metadata(
                    remote_schema_match=self.parquet_schema.equals(
                        remote_schema
                    )
                )

            # create parquet if no remote parquet found or remote schema
            # does not match expected Job schema
            if (
                file_info.type == fs.FileType.NotFound
                or not self.parquet_schema.equals(remote_schema)
            ):
                process_log.add_metadata(created=True, updated=False)
                self.create_parquet()
            # else attempt parquet update
            else:
                was_updated = self.update_parquet()
                process_log.add_metadata(created=False, updated=was_updated)

            parquet_file_size = os.path.getsize(self.local_parquet_path) / (
                1024 * 1024
            )

            process_log.add_metadata(
                parquet_file_size=f"{parquet_file_size:.2f}_MB"
            )

            if was_updated:
                self.upload_parquet()

            os.remove(self.local_parquet_path)

        except Exception as e:
            process_log.log_failure(exception=e)

        else:
            process_log.log_complete()
