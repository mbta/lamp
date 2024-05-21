import pyarrow

from lamp_py.tableau.hyper import HyperJob
from lamp_py.aws.s3 import download_file
from lamp_py.postgres.postgres_utils import DatabaseManager
from lamp_py.performance_manager.alerts import AlertParquetHandler


class HyperRtAlerts(HyperJob):
    """HyperJob for LAMP RT Alerts data"""

    def __init__(self) -> None:
        self.alert_pq_handler = AlertParquetHandler()
        HyperJob.__init__(
            self,
            hyper_file_name="LAMP_ALERTS.hyper",
            remote_parquet_path=f"s3://{self.alert_pq_handler.s3_path}",
            lamp_version=self.alert_pq_handler.file_version,
        )

    @property
    def parquet_schema(self) -> pyarrow.schema:
        return self.alert_pq_handler.parquet_schema

    def create_parquet(self, _: DatabaseManager) -> None:
        raise NotImplementedError("Should never create parquet")

    def update_parquet(self, _: DatabaseManager) -> bool:
        download_file(self.remote_parquet_path, self.local_parquet_path)
        return False
