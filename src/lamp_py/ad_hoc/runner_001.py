import os
from datetime import date
from datetime import timedelta

import pyarrow.parquet as pq

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.postgres.postgres_utils import start_rds_writer_process
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE
from lamp_py.runtime_utils.remote_files import LAMP
from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.aws.s3 import download_file
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter
from lamp_py.ingestion.converter import ConfigType


# pylint: disable=R0801
class AdHocConverter(GtfsRtConverter):
    """Custom ad-hoc Converter Class"""

    def convert(self) -> None:
        """ad-hoc Convert process to add revenue field to parquet files"""
        process_logger = ProcessLogger(
            "ad_hoc_converter",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        try:
            for table in self.process_files():
                if table.num_rows == 0:
                    continue

                self.continuous_pq_update(table)
                table_count += 1
                process_logger.add_metadata(table_count=table_count)

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.clean_local_folders()

    def sync_with_s3(self, local_path: str) -> bool:
        """
        sync local_path with S3 object if s3 file contains vehicle.trip.revenue columns

        :param local_path: local tmp path file to sync

        :return bool: True if local_path is available, else False
        """
        if os.path.exists(local_path):
            return True

        local_folder = local_path.replace(os.path.basename(local_path), "")
        os.makedirs(local_folder, exist_ok=True)

        s3_files = file_list_from_s3(
            S3_SPRINGBOARD,
            file_prefix=local_path.replace(f"{self.tmp_folder}/", ""),
        )
        if len(s3_files) == 1:
            s3_path = s3_files[0]
            pq_columns = pq.read_metadata(s3_path).schema.names
            if "vehicle.trip.revenue" in pq_columns:
                download_file(s3_path, local_path)
                return True

        return False


# pylint: enable=R0801


def runner() -> None:
    """add 'revenue' column to VehiclePosition Files"""
    ad_hoc_logger = ProcessLogger(process_name="ad_hoc_runner")
    ad_hoc_logger.log_start()
    prefix_date = date(2024, 7, 2)
    # start rds writer process
    # this will create only one rds engine while app is running
    metadata_queue, rds_process = start_rds_writer_process()

    while prefix_date < date(2024, 7, 8):
        prefix = (
            os.path.join(
                LAMP,
                "delta",
                prefix_date.strftime("%Y"),
                prefix_date.strftime("%m"),
                prefix_date.strftime("%d"),
            )
            + "/"
        )

        vp_file_list = file_list_from_s3(
            S3_ARCHIVE,
            prefix,
            in_filter="mbta.com_realtime_VehiclePositions_enhanced.json.gz",
        )

        converter = AdHocConverter(
            config_type=ConfigType.from_filename(vp_file_list[0]),
            metadata_queue=metadata_queue,
        )
        converter.add_files(vp_file_list)
        converter.convert()

        prefix_date += timedelta(days=1)

    # stop writer process
    metadata_queue.put(None)
    rds_process.join()
    ad_hoc_logger.log_complete()
