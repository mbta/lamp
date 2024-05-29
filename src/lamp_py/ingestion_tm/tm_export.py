import os
from abc import ABC
from abc import abstractmethod

import pyarrow

from lamp_py.mssql.mssql_utils import MSSQLManager


class TMExport(ABC):
    """
    Abstract Base Class for TM Export jobs
    """

    def __init__(
        self,
    ) -> None:
        self.export_bucket = os.getenv("SPRINGBOARD_BUCKET")

    @property
    @abstractmethod
    def export_schema(self) -> pyarrow.schema:
        """Schema for export"""

    @abstractmethod
    def run_export(self, tm_db: MSSQLManager) -> None:
        """
        Business logic to create new exprot parquet file
        """
