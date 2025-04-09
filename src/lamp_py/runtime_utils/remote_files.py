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

VERSION_KEY = "lamp_version"


@dataclass
class S3Location:
    """
    wrapper for a bucket name and prefix pair used to define an s3 location
    """

    bucket: str
    prefix: str
    version: str = "1.0"

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
########################################################
########################################################
# files ingested from delta - DEVGREEN
devgreen_rt_vehicle_positions = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "DEV_GREEN_RT_VEHICLE_POSITIONS"),
)

devgreen_rt_trip_updates = S3Location(
    bucket=S3_SPRINGBOARD,
    prefix=os.path.join(LAMP, "DEV_GREEN_RT_TRIP_UPDATES"),
)
########################################################
########################################################

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
bus_events = S3Location(bucket=S3_PUBLIC, prefix=os.path.join(LAMP, "bus_vehicle_events"), version="1.1")


# GTFS-RT feed filtered down by LAMP
# intermediate location
light_rail_events_dev_green_vehicle_pos = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/DEV_GREEN_RT_VEHICLE_POSITION")
)
light_rail_events_dev_green_trip_updates = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/DEV_GREEN_RT_TRIP_UPDATES")
)

light_rail_events_vehicle_pos = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/RT_VEHICLE_POSITIONS")
)
light_rail_events_trip_updates = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/RT_TRIP_UPDATES")
)


# Kinesis stream glides events
glides_trips_updated = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(LAMP, "GLIDES/trip_updates.parquet"), version="1.0"
)
glides_operator_signed_in = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(LAMP, "GLIDES/operator_sign_ins.parquet"), version="1.0"
)

public_alerts_file = S3Location(
    bucket=S3_PUBLIC,
    prefix=os.path.join(TABLEAU, "alerts", "LAMP_RT_ALERTS.parquet"),
)
tableau_rail = S3Location(
    bucket=S3_PUBLIC,
    prefix=os.path.join(TABLEAU, "rail"),
)
tableau_bus_recent = S3Location(bucket=S3_PUBLIC, prefix=os.path.join(TABLEAU, "bus", "LAMP_RECENT_Bus_Events.parquet"))
tableau_bus_all = S3Location(bucket=S3_PUBLIC, prefix=os.path.join(TABLEAU, "bus", "LAMP_ALL_Bus_Events.parquet"))


tableau_glides_all_operator_signed_in = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(TABLEAU, "glides", "LAMP_ALL_Glides_operator_sign_ins.parquet")
)
tableau_glides_all_trips_updated = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(TABLEAU, "glides", "LAMP_ALL_Glides_trip_updates.parquet")
)

#### GTFS-RT TO TABLEAU
# all of these files will contain up to the past 30 days of VehiclePosition or TripUpdates GTFS-RT data.
# This is updated once daily on a rolling basis
#
# light rail output file - to be converted to .hyper
tableau_rt_vehicle_positions_lightrail_60_day = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(TABLEAU, "gtfs-rt", "LAMP_RT_VehiclePositions_LR_60_day.parquet")
)
# light rail output file - to be converted to .hyper
tableau_rt_trip_updates_lightrail_60_day = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(TABLEAU, "gtfs-rt", "LAMP_RT_TripUpdates_LR_60_day.parquet")
)

# DEVGREEN
tableau_devgreen_rt_vehicle_positions_lightrail_60_day = S3Location(
    bucket=S3_ARCHIVE,
    prefix=os.path.join(TABLEAU, "devgreen-gtfs-rt", "LAMP_DEVGREEN_RT_VehiclePositions_LR_60_day.parquet"),
)
# light rail output file - to be converted to .hyper
tableau_devgreen_rt_trip_updates_lightrail_60_day = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(TABLEAU, "devgreen-gtfs-rt", "LAMP_DEVGREEN_RT_TripUpdates_LR_60_day.parquet")
)


#### GTFS-RT TO TABLEAU
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
