import os
from dataclasses import dataclass


@dataclass
class S3Location:
    """
    wrapper for a bucket name and prefix pair used to define an s3 location
    """

    bucket_name: str
    file_prefix: str

    def get_s3_path(self) -> str:
        """generate the full s3 url that can be used to describe the location"""
        return f"s3://{self.bucket_name}/{self.file_prefix}"


class RemoteFileLocations:
    """
    Constant S3 File Locations Used in Bus Performance Manager
    """

    # define buckets that we put our data into, both public and private
    springboard_bucket: str = os.environ.get("SPRINGBOARD_BUCKET", "")
    public_bucket: str = os.environ.get("PUBLIC_ARCHIVE_BUCKET", "")

    # files ingested from delta
    vehicle_positions = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join("lamp", "RT_VEHICLE_POSITIONS"),
    )

    # files ingested from tranist master
    tm_prefix = os.path.join("lamp", "TM")
    tm_stop_crossing = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "STOP_CROSSING"),
    )
    tm_daily_work_piece = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "DAILY_WORK_PIECE"),
    )
    tm_daily_sched_adherence_waiver_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(
            tm_prefix, "DAILY_SCHED_ADHERE_WAIVER.parquet"
        ),
    )
    tm_geo_node_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_GEO_NODE.parquet"),
    )
    tm_route_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_ROUTE.parquet"),
    )
    tm_trip_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_TRIP.parquet"),
    )
    tm_vehicle_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_VEHICLE.parquet"),
    )
    tm_operator_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_OPERATOR.parquet"),
    )
    tm_run_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_RUN.parquet"),
    )
    tm_block_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_BLOCK.parquet"),
    )
    tm_work_piece_file = S3Location(
        bucket_name=springboard_bucket,
        file_prefix=os.path.join(tm_prefix, "TMMAIN_WORK_PIECE.parquet"),
    )

    # output of public bus events published by LAMP
    bus_events = S3Location(
        bucket_name=public_bucket,
        file_prefix=os.path.join("lamp", "bus_vehicle_events"),
    )


def get_gtfs_parquet_file(year: int, filename: str) -> S3Location:
    """
    generate an S3Location instance for a gtfs schedule parquet file for a
    given type in a given year.
    """
    file_object = os.path.join("lamp", "gtfs_archive", str(year), filename)

    return S3Location(
        bucket_name=RemoteFileLocations.public_bucket, file_prefix=file_object
    )
