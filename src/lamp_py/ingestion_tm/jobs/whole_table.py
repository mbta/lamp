import os
import tempfile

import pyarrow
import sqlalchemy as sa

from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.mssql.mssql_utils import MSSQLManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import S3Location, RemoteFileLocations
from lamp_py.aws.s3 import upload_file


class TMWholeTable(TMExport):
    """Export Whole TM Table"""

    def __init__(
        self,
        s3_location: S3Location,
        tm_table: str,
    ) -> None:
        self.s3_location = s3_location
        self.tm_table = tm_table

    def run_export(self, tm_db: MSSQLManager) -> None:
        table_columns = ",".join([col.name for col in self.export_schema])
        query = sa.text(f"SELECT {table_columns} FROM {self.tm_table};")

        logger = ProcessLogger(
            process_name="tm_whole_table_export",
            tm_table=self.tm_table,
        )
        logger.log_start()
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                local_export_path = os.path.join(temp_dir, "out.parquet")
                tm_db.write_to_parquet(
                    query, local_export_path, self.export_schema
                )
                logger.add_metadata(
                    pq_export_bytes=os.stat(local_export_path).st_size
                )
                upload_file(local_export_path, self.s3_location.get_s3_path())
                logger.log_complete()

        except Exception as exception:
            logger.log_failure(exception)


class TMMainGeoNode(TMWholeTable):
    """Export GEO_NODE table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_geo_node_file,
            tm_table="TMMain.dbo.GEO_NODE",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("GEO_NODE_ID", pyarrow.int64()),
                ("DEPARTURE_ZONE_ID", pyarrow.int64()),
                ("ARRIVAL_ZONE_ID", pyarrow.int64()),
                ("INTERNAL_ANNOUNCEMENT_ID", pyarrow.int64()),
                ("GEO_NODE_ABBR", pyarrow.string()),
                ("GEO_NODE_NAME", pyarrow.string()),
                ("TRANSIT_DIV_ID", pyarrow.string()),
                ("LATITUDE", pyarrow.int64()),
                ("LONGITUDE", pyarrow.int64()),
                ("ALTITUDE", pyarrow.string()),
                ("DETOUR_IND", pyarrow.string()),
                ("DETOUR_LATITUDE", pyarrow.int64()),
                ("DETOUR_LONGITIUDE", pyarrow.int64()),
                ("REMARK", pyarrow.string()),
                ("DIFFERENTIAL_VALIDITY", pyarrow.int64()),
                ("NUM_OF_SATELLITES", pyarrow.int64()),
                ("ACTIVATION_DATE", pyarrow.string()),
                ("DEACTIVATION_DATE", pyarrow.string()),
                ("MAP_LATITUDE", pyarrow.int64()),
                ("MAP_LONGITUDE", pyarrow.int64()),
                ("ANNOUNCEMENT_ZONE_ID", pyarrow.int64()),
                ("NUM_SURVEY_POINTS", pyarrow.int64()),
                ("USE_SURVEY", pyarrow.bool_()),
                ("MDT_LATITUDE", pyarrow.int64()),
                ("MDT_LONGITUDE", pyarrow.int64()),
                ("LOCK_MAP", pyarrow.bool_()),
                ("GEO_NODE_PUBLIC_NAME", pyarrow.string()),
                ("SOURCE_STOP_ID", pyarrow.int64()),
                ("PUBLIC_STOP_NUMBER", pyarrow.string()),
                ("BAY_INFO", pyarrow.string()),
            ]
        )


class TMMainTrip(TMWholeTable):
    """Export TRIP table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_trip_file,
            tm_table="TMMain.dbo.TRIP",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("TRIP_ID", pyarrow.int64()),
                ("EXTERNAL_ANNOUNCEMENT_ID", pyarrow.int64()),
                ("WORK_PIECE_ID", pyarrow.int64()),
                ("TRIP_TYPE_ID", pyarrow.int64()),
                ("TIME_TABLE_VERSION_ID", pyarrow.int64()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("BLOCK_ID", pyarrow.int64()),
                ("TRIP_SERIAL_NUMBER", pyarrow.int64()),
                ("TRIP_SEQUENCE", pyarrow.int64()),
                ("REMARK", pyarrow.string()),
                ("BLOCK_TRIP_SEQ", pyarrow.int64()),
                ("Pattern_ID", pyarrow.int64()),
                ("TRIP_START_NODE_ID", pyarrow.int64()),
                ("TRIP_END_TIME", pyarrow.int64()),
                ("TRIP_END_NODE_ID", pyarrow.int64()),
                ("SOURCE_TRIP_ID", pyarrow.int64()),
                ("EXC_COMBO_ID", pyarrow.int64()),
            ]
        )


class TMMainRoute(TMWholeTable):
    """Export ROUTE table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_route_file,
            tm_table="TMMain.dbo.ROUTE",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("ROUTE_ID", pyarrow.int64()),
                ("ROUTE_GROUP_ID", pyarrow.int64()),
                ("ROUTE_ABBR", pyarrow.string()),
                ("ROUTE_NAME", pyarrow.string()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("TIME_TABLE_VERSION_ID", pyarrow.int64()),
                ("SOURCE_LINE_ID", pyarrow.int64()),
                ("MASTER_ROUTE_ID", pyarrow.int64()),
            ]
        )


class TMMainVehicle(TMWholeTable):
    """Export VEHICLE table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_vehicle_file,
            tm_table="TMMain.dbo.VEHICLE",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("VEHICLE_ID", pyarrow.int64()),
                ("FLEET_ID", pyarrow.int64()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("PROPERTY_TAG", pyarrow.string()),
                ("MFG_MODEL_SERIES_ID", pyarrow.int64()),
                ("VEH_SERVICE_STATUS_ID", pyarrow.int64()),
                ("VEHICLE_TYPE_ID", pyarrow.int64()),
                ("VEHICLE_BASE_ID", pyarrow.int64()),
                ("RNET_ADDRESS", pyarrow.int64()),
                ("VIN", pyarrow.string()),
                ("LICENSE_PLATE", pyarrow.string()),
                ("MODEL_YEAR", pyarrow.int64()),
                ("ODOMETER_MILEAGE", pyarrow.int64()),
                ("ODOMTR_UPDATE_TIMESTAMP", pyarrow.timestamp("ms")),
                ("ODOMTR_UPDATE_SOURCE_IND", pyarrow.string()),
                ("GENERAL_INFORMATION", pyarrow.string()),
                ("LOAD_ID", pyarrow.int64()),
                ("RADIO_LID", pyarrow.int64()),
                ("ENGINE_CONTROLLER_ID", pyarrow.int64()),
                ("DEFAULT_VOICE_CHANNEL", pyarrow.int64()),
                ("USE_DHCP", pyarrow.bool_()),
                ("IP_OCT_1", pyarrow.int64()),
                ("IP_OCT_2", pyarrow.int64()),
                ("IP_OCT_3", pyarrow.int64()),
                ("IP_OCT_4", pyarrow.int64()),
                ("EEPROM_TEMPLATE_ID", pyarrow.int64()),
                ("DECOMMISSION", pyarrow.bool_()),
                ("VEHICLE_TYPE_DESCRIPTION_ID", pyarrow.string()),
                ("APC_TYPE_ID", pyarrow.int64()),
                ("RADIO_CONFIGURATION_ID", pyarrow.int64()),
                ("VOICE_CAPABILITY", pyarrow.int64()),
                ("IVLU_FILE_PACKAGING", pyarrow.int64()),
                ("IVLU_WAKEUP_MESSAGE", pyarrow.int64()),
                ("APC_ENABLED", pyarrow.bool_()),
                ("COLLECT_APC_DATA", pyarrow.bool_()),
                ("TSP_DEVICE_ID", pyarrow.string()),
                ("RADIO_DEVICE_ID", pyarrow.string()),
            ]
        )


class TMMainOperator(TMWholeTable):
    """Export OPERATOR table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_operator_file,
            tm_table="TMMain.dbo.OPERATOR",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("OPERATOR_ID", pyarrow.int64()),
                ("ONBOARD_LOGON_ID", pyarrow.int64()),
                ("BADGE", pyarrow.string()),
                ("FIRST_NAME", pyarrow.string()),
                ("MIDDLE_NAME", pyarrow.string()),
                ("LAST_NAME", pyarrow.string()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("DIVISION", pyarrow.string()),
                ("DEPARTMENT", pyarrow.string()),
                ("ACTIVATION_DATE", pyarrow.timestamp("ms")),
                ("DEACTIVATION_DATE", pyarrow.timestamp("ms")),
                ("OPERATOR_TYPE_ID", pyarrow.int64()),
            ]
        )


class TMMainRun(TMWholeTable):
    """Export RUN table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_run_file,
            tm_table="TMMain.dbo.RUN",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("RUN_ID", pyarrow.int64()),
                ("RUN_DESIGNATOR", pyarrow.string()),
                ("RUN_TYPE_ID", pyarrow.int64()),
                ("SERVICE_TYPE_ID", pyarrow.int64()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("TIME_TABLE_VERSION_ID", pyarrow.int64()),
                ("RUN_NUM", pyarrow.int64()),
                ("MDT_RUN_ID", pyarrow.int64()),
                ("MASTER_RUN_ID", pyarrow.int64()),
            ]
        )


class TMMainBlock(TMWholeTable):
    """Export BLOCK table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_block_file,
            tm_table="TMMain.dbo.BLOCK",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("BLOCK_ID", pyarrow.int64()),
                ("TIME_TABLE_VERSION_ID", pyarrow.int64()),
                ("VEHICLE_ID", pyarrow.int64()),
                ("TRANSIT_DIV_ID", pyarrow.int64()),
                ("BLOCK_ABBR", pyarrow.string()),
                ("BLOCK_NUM", pyarrow.int64()),
                ("SERVICE_TYPE_ID", pyarrow.int64()),
                ("VEHICLE_TYPE_ID", pyarrow.int64()),
                ("PADDLE_NOTES", pyarrow.string()),
                ("MDT_BLOCK_ID", pyarrow.int64()),
                ("SOURCE_BLOCK_ID", pyarrow.int64()),
                ("MASTER_BLOCK_ID", pyarrow.int64()),
                ("VEHICLE_ATTRIBUTES_REQUIRED", pyarrow.string()),
                ("OPERATING_MODE_ID", pyarrow.int64()),
                ("AGENCY_ID", pyarrow.int64()),
            ]
        )


class TMMainWorkPiece(TMWholeTable):
    """Export WORK_PIECE table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_work_piece_file,
            tm_table="TMMain.dbo.WORK_PIECE",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("WORK_PIECE_ID", pyarrow.int64()),
                ("TIME_TABLE_VERSION_ID", pyarrow.int64()),
                ("RUN_ID", pyarrow.int64()),
                ("OPERATING_TIME_ID", pyarrow.int64()),
                ("PAY_ID", pyarrow.int64()),
                ("BEGIN_TIME", pyarrow.int64()),
                ("END_TIME", pyarrow.int64()),
                ("BLOCK_ID", pyarrow.int64()),
                ("SERVICE_TYPE_ID", pyarrow.int64()),
                ("REPORT_TIME", pyarrow.int64()),
                ("CLEAR_TIME", pyarrow.int64()),
                ("EXC_COMBO_ID", pyarrow.int64()),
                ("AGENCY_ID", pyarrow.int64()),
            ]
        )


class TMDailyLogDailySchedAdhereWaiver(TMWholeTable):
    """Export SCHED_ADHERE_WAIVER table from TMDailyLog"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            s3_location=RemoteFileLocations.tm_daily_sched_adherence_waiver_file,
            tm_table="TMDailyLog.dbo.SCHED_ADHERE_WAIVER",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("WAIVER_ID", pyarrow.int64()),
                ("CALENDAR_ID", pyarrow.int64()),
                ("EARLY_ALLOWED_FLAG", pyarrow.int8()),
                ("LATE_ALLOWED_FLAG", pyarrow.int8()),
                ("CREATE_BY_DISPATCH_ID", pyarrow.int64()),
                ("CREATE_DATETIME", pyarrow.timestamp("ms")),
                ("ENDED_BY_DISPATCH_ID", pyarrow.int64()),
                ("ENDED_DATE_TIME", pyarrow.timestamp("ms")),
                ("REMARK", pyarrow.string()),
                ("UPDATE_TIMESTAMP", pyarrow.timestamp("ms")),
                ("MISSED_ALLOWED_FLAG", pyarrow.int8()),
                ("NO_REVENUE_FLAG", pyarrow.bool_()),
                ("WAIVER_TIMEOUT", pyarrow.int64()),
                ("SCHEDULED_WAIVER_ID", pyarrow.int64()),
                ("SERVICE_NOTICE_CAUSE_ID", pyarrow.int64()),
            ]
        )
