import os
from dataclasses import dataclass

import pyarrow
from pyarrow import csv, parquet

test_files_dir = os.path.join(os.path.dirname(__file__), "test_files")

incoming_dir = os.path.join(test_files_dir, "INCOMING")
springboard_dir = os.path.join(test_files_dir, "SPRINGBOARD")


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


@dataclass
class LocalS3Location:
    """replace an s3 location wrapper class so it can be used in testing"""

    bucket_name: str
    file_prefix: str

    def get_s3_path(self) -> str:
        """generate the local path to the test file for this object"""
        return os.path.join(test_files_dir, self.bucket_name, self.file_prefix)


class LocalFileLocaions:
    """Replace S3 File Locations with Local Files"""

    springboard_bucket = "SPRINGBOARD"
    public_bucket = "PUBLIC_ARCHIVE"

    # files ingested from delta
    vehicle_positions = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix="RT_VEHICLE_POSITIONS",
    )

    # files ingested from tranist master
    tm_prefix = "TM"
    tm_stop_crossing = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "STOP_CROSSING"),
    )
    tm_geo_node_file = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_GEO_NODE.parquet"),
    )
    tm_route_file = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_ROUTE.parquet"),
    )
    tm_trip_file = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_TRIP.parquet"),
    )
    tm_vehicle_file = LocalS3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_VEHICLE.parquet"),
    )

    # output of public bus events published by LAMP
    bus_events = LocalS3Location(
        bucket_name=public_bucket,
        file_prefix="bus_vehicle_events",
    )


def get_local_gtfs_parquet_files(year: int, filename: str) -> LocalS3Location:
    """Replace get_gtfs_parquet_files to work on local files."""
    file_object = os.path.join("gtfs_archive", str(year), filename)

    return LocalS3Location(
        bucket_name=LocalFileLocaions.public_bucket, file_prefix=file_object
    )
