import pyarrow

from lamp_py.aws.s3 import download_file
from lamp_py.performance_manager.alerts import AlertsS3Info
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.tableau.hyper import HyperJob


class HyperRtAlerts(HyperJob):
    """HyperJob for LAMP Alerts dataset"""

    def __init__(self) -> None:
        HyperJob.__init__(
            self,
            hyper_file_name="LAMP_ALERTS.hyper",
            remote_parquet_path=AlertsS3Info.s3_path,
            lamp_version=AlertsS3Info.file_version,
        )

    @property
    def output_processed_schema(self) -> pyarrow.schema:
        return AlertsS3Info.parquet_schema

    def create_parquet(self, _: DatabaseManager | None) -> None:
        raise NotImplementedError("Alerts Hyper Job does not create parquet file")

    def update_parquet(self, _: DatabaseManager | None) -> bool:
        download_file(
            object_path=self.remote_parquet_path,
            file_name=self.local_parquet_path,
        )
        return False
