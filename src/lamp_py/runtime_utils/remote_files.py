import os
from dataclasses import dataclass


@dataclass
class S3Location:
    """
    wrapper for a bucket name and prefix pair used to define an s3 location
    """

    bucket_name: str
    file_prefix: str


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

    # output of public bus events published by LAMP
    bus_events = S3Location(
        bucket_name=public_bucket,
        file_prefix=os.path.join("lamp", "bus_vehicle_events"),
    )
