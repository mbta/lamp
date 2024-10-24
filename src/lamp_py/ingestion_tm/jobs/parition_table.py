import os
import re
import tempfile
from typing import (
    List,
    Optional,
)

import pyarrow
import sqlalchemy as sa

from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.mssql.mssql_utils import MSSQLManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    S3Location,
    tm_stop_crossing,
    tm_daily_work_piece,
)

from lamp_py.aws.s3 import (
    file_list_from_s3,
    upload_file,
    object_metadata,
)


class TMDailyTable(TMExport):
    """Export Daily table from TMDailyLog"""

    def __init__(
        self,
        s3_location: S3Location,
        tm_table: str,
        lamp_version: str,
    ) -> None:
        self.s3_location = s3_location
        self.tm_table = tm_table
        self.lamp_version = lamp_version
        self.version_key = "lamp_version"
        self.s3_version_path = os.path.join(
            self.s3_location.s3_uri,
            self.version_key,
        )

    def update_version_file(self) -> None:
        """write version file to s3 for partition dataset"""
        with tempfile.TemporaryDirectory() as temp_dir:
            local_version = os.path.join(temp_dir, self.version_key)

            with open(local_version, "w", encoding="utf8") as f:
                f.write(self.lamp_version)

            upload_file(
                file_name=local_version,
                object_path=self.s3_version_path,
                extra_args={"Metadata": {self.version_key: self.lamp_version}},
            )

    def dates_from_tm(self, tm_db: MSSQLManager) -> List[int]:
        """
        retrieve dates to process from Transit Master database

        returns sorted list of CALENDAR_ID dates

        :return [120240101, 120240102]
        """
        tm_dates_query = sa.text(f"SELECT DISTINCT CALENDAR_ID FROM {self.tm_table};")
        tm_dates = tm_db.select_as_dataframe(tm_dates_query)

        return sorted(tm_dates["CALENDAR_ID"].astype(int).to_list())

    def dates_from_s3(self) -> List[int]:
        """
        retrive list of already exported dates from S3 prefix

        returns sorted list of CALENDAR_ID extracted from paths

        :return [120240101, 120240102]
        """
        s3_files = file_list_from_s3(self.s3_location.bucket, self.s3_location.prefix)

        def date_match(s: str) -> Optional[int]:
            match = re.search(r"\d{9}", s)
            if match:
                return int(match.group())
            return None

        return sorted([s for s in map(date_match, s3_files) if s])

    def dates_to_export(self, tm_db: MSSQLManager) -> List[int]:
        """
        compare S3 exported file dates to available dates in Transit Master
        export any dates from Transit Master that are not found in S3 as well as
        the last two dates found in Transit Master

        if expected lamp_version does not match, return all available Transit Master dates

        :return [120240101, 120240102]
        """
        s3_version = None
        tm_dates = self.dates_from_tm(tm_db)

        version_file = file_list_from_s3(
            self.s3_location.bucket,
            os.path.join(self.s3_location.prefix, self.version_key),
        )
        if len(version_file) == 1:
            s3_version = object_metadata(self.s3_version_path).get(self.version_key)

        if s3_version != self.lamp_version:
            return tm_dates

        s3_dates = self.dates_from_s3()

        export_dates = set(tm_dates).difference(set(s3_dates))
        export_dates.update(tm_dates[-2:])

        return sorted(export_dates)

    def run_export(self, tm_db: MSSQLManager) -> None:
        table_columns = ",".join([col.name for col in self.export_schema])

        for date in self.dates_to_export(tm_db):
            try:
                logger = ProcessLogger("tm_daily_log_export", tm_table=self.tm_table, date=date)
                logger.log_start()

                query = sa.text(
                    f"""
                    SELECT
                        {table_columns}
                    FROM
                        {self.tm_table}
                    WHERE 
                        CALENDAR_ID = {date}
                    ;
                    """
                )

                s3_export_path = os.path.join(
                    self.s3_location.s3_uri,
                    f"{date}.parquet",
                )

                with tempfile.TemporaryDirectory() as temp_dir:
                    local_pq = os.path.join(temp_dir, "out.parquet")
                    tm_db.write_to_parquet(
                        select_query=query,
                        write_path=local_pq,
                        schema=self.export_schema,
                    )
                    logger.add_metadata(
                        last_export_path=s3_export_path,
                        last_export_bytes=os.stat(local_pq).st_size,
                    )
                    upload_file(local_pq, s3_export_path)

                self.update_version_file()
                logger.log_complete()

            except Exception as exception:
                logger.log_failure(exception)


class TMDailyLogStopCrossing(TMDailyTable):
    """Export STOP_CROSSING table from TMDailyLog"""

    def __init__(self) -> None:
        TMDailyTable.__init__(
            self,
            s3_location=tm_stop_crossing,
            tm_table="TMDailyLog.dbo.STOP_CROSSING",
            lamp_version="0.0.1",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("STOP_CROSSING_ID", pyarrow.int64()),
                ("TRIP_GEO_NODE_XREF_ID", pyarrow.int64()),
                ("PATTERN_GEO_NODE_SEQ", pyarrow.int64()),
                ("CALENDAR_ID", pyarrow.int64()),
                ("ROUTE_DIRECTION_ID", pyarrow.int64()),
                ("PATTERN_ID", pyarrow.int64()),
                ("GEO_NODE_ID", pyarrow.int64()),
                ("BLOCK_STOP_ORDER", pyarrow.int64()),
                ("SCHEDULED_TIME", pyarrow.int64()),
                ("ACT_ARRIVAL_TIME", pyarrow.int64()),
                ("ACT_DEPARTURE_TIME", pyarrow.int64()),
                ("ODOMETER", pyarrow.int64()),
                ("WAIVER_ID", pyarrow.int64()),
                ("DAILY_WORK_PIECE_ID", pyarrow.int64()),
                ("TIME_POINT_ID", pyarrow.int64()),
                ("SERVICE_TYPE_ID", pyarrow.int64()),
                ("VEHICLE_ID", pyarrow.int64()),
                ("TRIP_ID", pyarrow.int64()),
                ("PULLOUT_ID", pyarrow.int64()),
                ("IsRevenue", pyarrow.string()),
                ("SCHEDULE_TIME_OFFSET", pyarrow.int64()),
                ("CROSSING_TYPE_ID", pyarrow.int64()),
                ("OPERATOR_ID", pyarrow.int64()),
                ("CANCELLED_FLAG", pyarrow.bool_()),
                ("IS_LAYOVER", pyarrow.bool_()),
                ("ROUTE_ID", pyarrow.int64()),
            ]
        )


class TMDailyLogDailyWorkPiece(TMDailyTable):
    """Export DAILY_WORK_PIECE table from TMDailyLog"""

    def __init__(self) -> None:
        TMDailyTable.__init__(
            self,
            s3_location=tm_daily_work_piece,
            tm_table="TMDailyLog.dbo.DAILY_WORK_PIECE",
            lamp_version="0.0.1",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("DAILY_WORK_PIECE_ID", pyarrow.int64()),
                ("WORK_PIECE_ID", pyarrow.int64()),
                ("CALENDAR_ID", pyarrow.int64()),
                ("ASSIGNED_OPERATOR_ID", pyarrow.int64()),
                ("CURRENT_OPERATOR_ID", pyarrow.int64()),
                ("SCHEDULED_LOGON_TIME", pyarrow.int64()),
                ("SCHEDULED_LOGOFF_TIME", pyarrow.int64()),
                ("ACTUAL_LOGON_TIME", pyarrow.int64()),
                ("ACTUAL_LOGOFF_TIME", pyarrow.int64()),
                ("ALARM_STATUS_ID", pyarrow.int64()),
                ("PULLOUT_ID", pyarrow.int64()),
                ("RUN_ID", pyarrow.int64()),
                ("ASSIGNED_VEHICLE_ID", pyarrow.int64()),
                ("CURRENT_VEHICLE_ID", pyarrow.int64()),
                ("INSERTED_FLAG", pyarrow.bool_()),
            ]
        )
