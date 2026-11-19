import os
from dataclasses import dataclass

import pyarrow
from pyarrow import csv, parquet

from lamp_py.runtime_utils.remote_files import S3_SPRINGBOARD, S3_INCOMING, S3Location

test_files_dir = os.path.join(os.path.dirname(__file__), "test_files")


def csv_to_vp_parquet(csv_filepath: str, parquet_filepath: str) -> None:
    """
    read vehicle position data in csv format and write it to a parquet file
    """
    vp_csv_options = csv.ConvertOptions(
        column_types={
            "vehicle.current_status": pyarrow.string(),
            "vehicle.current_stop_sequence": pyarrow.uint32(),
            "vehicle.stop_id": pyarrow.string(),
            "vehicle.timestamp": pyarrow.uint64(),
            "vehicle.trip.direction_id": pyarrow.uint8(),
            "vehicle.trip.route_id": pyarrow.string(),
            "vehicle.trip.trip_id": pyarrow.string(),
            "vehicle.trip.start_date": pyarrow.string(),
            "vehicle.trip.start_time": pyarrow.string(),
            "vehicle.vehicle.id": pyarrow.string(),
            "vehicle.vehicle.consist": pyarrow.string(),
        },
        # in our ingestion, if a key is missing, the value written to the
        # parquet file is null. mimic this behavior by making empty strings
        # null instead of ''.
        strings_can_be_null=True,
    )

    table = csv.read_csv(csv_filepath, convert_options=vp_csv_options)
    parquet.write_table(table, parquet_filepath)


incoming_dir = os.path.join(test_files_dir, S3_INCOMING)
springboard_dir = os.path.join(test_files_dir, S3_SPRINGBOARD)


@dataclass
class LocalS3Location(S3Location):
    """replace an s3 location wrapper class so it can be used in testing"""

    bucket: str
    prefix: str

    @property
    def s3_uri(self) -> str:
        """generate the local path to the test file for this object"""
        return os.path.join(test_files_dir, self.bucket, self.prefix)


rt_vehicle_positions = LocalS3Location(
    bucket=S3_SPRINGBOARD,
    prefix="RT_VEHICLE_POSITIONS",
)

tm_stop_crossings = LocalS3Location(
    bucket=S3_SPRINGBOARD,
    prefix="TM/STOP_CROSSING",
)
tm_daily_logged_message = LocalS3Location(
    bucket=S3_SPRINGBOARD,
    prefix="TM/DAILY_LOGGED_MESSAGE",
)
tm_geo_node_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_GEO_NODE.parquet")
tm_route_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_ROUTE.parquet")
tm_trip_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_TRIP.parquet")
tm_vehicle_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_VEHICLE.parquet")
tm_pattern_geo_node_xref_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_PATTERN_GEO_NODE_XREF.parquet")
tm_time_point_file = LocalS3Location(bucket=S3_SPRINGBOARD, prefix="TM/TMMAIN_TIME_POINT.parquet")
tm_daily_sched_adherence_waiver_file = LocalS3Location(
    bucket=S3_SPRINGBOARD,
    prefix="TM/DAILY_SCHED_ADHERE_WAIVER.parquet",
)
