import os
from dataclasses import dataclass
from typing import Union

# bucket constants
S3_SPRINGBOARD: str = os.environ.get("SPRINGBOARD_BUCKET", "unset_SPRINGBOARD")
S3_PUBLIC: str = os.environ.get("PUBLIC_ARCHIVE_BUCKET", "unset_PUBLIC")
S3_INCOMING: str = os.environ.get("INCOMING_BUCKET", "unset_INCOMING")
S3_ARCHIVE: str = os.environ.get("ARCHIVE_BUCKET", "unset_ARCHIVE")
S3_ERROR: str = os.environ.get("ERROR_BUCKET", "unset_ERROR")

# prefix constants
LAMP = "lamp"
TM = os.path.join(LAMP, "TM")
TABLEAU = os.path.join(LAMP, "tableau")


@dataclass
class S3Location:
    """
    wrapper for a bucket name and prefix pair used to define an s3 location
    """

    bucket: str
    prefix: str

    @property
    def s3_uri(self) -> str:
        """generate the full s3 uri for the location"""
        return f"s3://{self.bucket}/{self.prefix}"


# files ingested from delta
rt_vehicle_positions = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "RT_VEHICLE_POSITIONS"),
)

rt_trip_updates = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "RT_TRIP_UPDATES"),
)

rt_alerts = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "RT_ALERTS"),
)

bus_vehicle_positions = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "BUS_VEHICLE_POSITIONS"),
)

bus_trip_updates = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "BUS_TRIP_UPDATES"),
)

# files ingested from transit master
tm_stop_crossing = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "STOP_CROSSING"),
)
tm_daily_work_piece = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "DAILY_WORK_PIECE"),
)
tm_daily_sched_adherence_waiver_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "DAILY_SCHED_ADHERE_WAIVER.parquet"),
)
tm_geo_node_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_GEO_NODE.parquet"),
)
tm_route_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_ROUTE.parquet"),
)
tm_trip_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_TRIP.parquet"),
)
tm_vehicle_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_VEHICLE.parquet"),
)
tm_operator_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_OPERATOR.parquet"),
)
tm_run_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_RUN.parquet"),
)
tm_block_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_BLOCK.parquet"),
)
tm_work_piece_file = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(TM, "TMMAIN_WORK_PIECE.parquet"),
)

# published by LAMP
bus_events = S3Location(
    bucket=S3_PUBLIC,
    prefix=os.path.join(LAMP, "bus_vehicle_events"),
)
public_alerts_file = S3Location(
    bucket=S3_PUBLIC,
    prefix=os.path.join(TABLEAU, "alerts", "LAMP_RT_ALERTS.parquet"),
)
tableau_rail = S3Location(
    bucket=S3_PUBLIC, prefix=os.path.join(TABLEAU, "rail")
)


class GTFSArchive(S3Location):
    """S3Location for Compressed GTFS Archive"""

    def __init__(self, bucket: str, prefix: str) -> None:
        S3Location.__init__(self, bucket, prefix)

    def parquet_path(self, year: Union[str, int], file: str) -> S3Location:
        """
        produce parquet S3Location with year and file for specific GTFS archive

        :param year: archive year
        :param file: GTFS file type (e.g. routes)
        """
        file = file.replace(".parquet", "")
        return S3Location(
            bucket=self.bucket,
            prefix=os.path.join(self.prefix, str(year), f"{file}.parquet"),
        )


compressed_gtfs = GTFSArchive(
    bucket=S3_PUBLIC,
    prefix=os.path.join(LAMP, "gtfs_archive"),
)