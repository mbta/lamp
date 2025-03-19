import os
from abc import ABC
from abc import abstractmethod
from itertools import chain
from typing import Dict
from typing import Optional

import pyarrow
from pyarrow import fs
import pyarrow.parquet as pq

from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import (
    download_file,
    upload_file,
    object_metadata,
)

class Converter(ABC):  # pylint: disable=R0902
    """
    Abstract Base Class for Parquet jobs
    """

    def __init__(
        self,
        hyper_file_name: str,
        remote_parquet_path: str,
        lamp_version: str,
        project_name: str = os.getenv("TABLEAU_PROJECT", ""),
    ) -> None:
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
    def parquet_schema(self) -> pyarrow.schema:
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
                remote_schema_match = self.parquet_schema.equals(remote_schema)
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
