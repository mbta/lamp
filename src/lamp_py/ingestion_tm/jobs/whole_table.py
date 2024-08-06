import os
import tempfile

import pyarrow
import sqlalchemy as sa

from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.mssql.mssql_utils import MSSQLManager
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import upload_file


class TMWholeTable(TMExport):
    """Export Whole TM Table"""

    def __init__(
        self,
        pq_file_name: str,
        tm_table: str,
    ) -> None:
        TMExport.__init__(self)

        self.tm_table = tm_table
        self.remote_parquet_path = (
            f"s3://{self.export_bucket}/lamp/TM/{pq_file_name}"
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        """Schema for export"""

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
                upload_file(local_export_path, self.remote_parquet_path)
                logger.log_complete()

        except Exception as exception:
            logger.log_failure(exception)


class TMMainGeoNode(TMWholeTable):
    """Export GEO_NODE table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            pq_file_name="TMMAIN_GEO_NODE.parquet",
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
            pq_file_name="TMMAIN_TRIP.parquet",
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
            pq_file_name="TMMAIN_ROUTE.parquet",
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
            pq_file_name="TMMAIN_VEHICLE.parquet",
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


class TMMainsScheduledWaiver(TMWholeTable):
    """Export SCHEDULED_WAIVER table from TMMain"""

    def __init__(self) -> None:
        TMWholeTable.__init__(
            self,
            pq_file_name="TMMAIN_SCHEDULED_WAIVER.parquet",
            tm_table="TMMain.dbo.SCHEDULED_WAIVER",
        )

    @property
    def export_schema(self) -> pyarrow.schema:
        return pyarrow.schema(
            [
                ("SCHEDULED_WAIVER_ID", pyarrow.int64()),
                ("WAIVER_DESCRIPTION", pyarrow.string()),
                ("ROUTE_ABBR", pyarrow.string()),
                ("ROUTE_DIRECTION_ID", pyarrow.int64()),
                ("START_TIME", pyarrow.int64()),
                ("END_TIME", pyarrow.int64()),
                ("ACTIVATION_DATE", pyarrow.timestamp("ms")),
                ("MISSED_ALLOWED_FLAG", pyarrow.bool_()),
                ("DEACTIVATION_DATE", pyarrow.timestamp("ms")),
                ("EARLY_ALLOWED_FLAG", pyarrow.bool_()),
                ("LATE_ALLOWED_FLAG", pyarrow.bool_()),
            ]
        )
