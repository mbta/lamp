import re
from datetime import timedelta, date
from typing import List, Tuple, Dict

from lamp_py.aws.s3 import (
    file_list_after,
    file_list_from_s3,
    get_datetime_from_partition_path,
    get_last_modified_object,
)

from lamp_py.runtime_utils.remote_files import RemoteFileLocations


class BusInputFiles:
    """
    small dataclass to keep track of vp files and tm files for a given service
    date
    """

    def __init__(self, service_date: date):
        self.service_date: date = service_date
        self.vp_files: List[str] = []
        self.tm_files: List[str] = []


def get_new_event_files() -> Tuple[List[str], List[str]]:
    """
    get new realtime files from the vehicle position and transit master s3
    locations that have been populated since the last time vehicle events were
    processed.
    """
    latest_event_file = get_last_modified_object(
        bucket_name=RemoteFileLocations.bus_events.bucket_name,
        file_prefix=RemoteFileLocations.bus_events.file_prefix,
        version="1.0",
    )

    if latest_event_file:
        cutoff = latest_event_file["last_modified"] - timedelta(hours=1)
        vp_files = file_list_after(
            bucket_name=RemoteFileLocations.vehicle_positions.bucket_name,
            file_prefix=RemoteFileLocations.vehicle_positions.file_prefix,
            cutoff=cutoff,
        )

        tm_files = file_list_after(
            bucket_name=RemoteFileLocations.tm_stop_crossing.bucket_name,
            file_prefix=RemoteFileLocations.tm_stop_crossing.file_prefix,
            cutoff=cutoff,
        )

    else:
        vp_files = file_list_from_s3(
            bucket_name=RemoteFileLocations.vehicle_positions.bucket_name,
            file_prefix=RemoteFileLocations.vehicle_positions.file_prefix,
        )

        tm_files = file_list_from_s3(
            bucket_name=RemoteFileLocations.tm_stop_crossing.bucket_name,
            file_prefix=RemoteFileLocations.tm_stop_crossing.file_prefix,
        )

    return vp_files, tm_files


def files_per_service_date(
    vp_files: List[str], tm_files: List[str]
) -> Dict[date, BusInputFiles]:
    """
    take a list of input files and bucket them based on the service date they
    cover.
    """

    service_dates: Dict[date, BusInputFiles] = {}

    def add_vp_file(service_date: date, vp_file: str) -> None:
        """helper to add vp file to service dates"""
        if service_date not in service_dates:
            service_dates[service_date] = BusInputFiles(
                service_date=service_date
            )

        service_dates[service_date].vp_files.append(vp_file)

    def add_tm_file(service_date: date, tm_file: str) -> None:
        """helper to add tm file to service dates"""
        if service_date not in service_dates:
            service_dates[service_date] = BusInputFiles(
                service_date=service_date
            )

        service_dates[service_date].tm_files.append(tm_file)

    # parse vp filepaths to infer service dates from the partition paths
    for vp_file in vp_files:
        file_datetime = get_datetime_from_partition_path(vp_file)

        # if the partition is hourly (for older files), consider the hour of
        # the file to determine the service date from the file datetime
        if "hour" in vp_file:
            if file_datetime.hour < 6:
                yesterday = (file_datetime - timedelta(days=1)).date()
                add_vp_file(yesterday, vp_file)
            if file_datetime.hour > 3:
                add_vp_file(file_datetime.date(), vp_file)
        # if the partition is daily (for newer files), there will be data for
        # the service date in the partition path and for data in the previous
        # service date.
        else:
            yesterday = (file_datetime - timedelta(days=1)).date()
            add_vp_file(yesterday, vp_file)
            add_vp_file(file_datetime.date(), vp_file)

    # parse the tm files to get the service date from the filename
    for tm_file in tm_files:
        try:
            # files are formatted "1YYYYMMDD.parquet"
            service_date_int = re.findall(r"(\d{8}).parquet", tm_file)[0]
            year = int(service_date_int[:4])
            month = int(service_date_int[4:6])
            day = int(service_date_int[6:])

            service_date = date(year=year, month=month, day=day)
            add_tm_file(service_date, tm_file)
        except IndexError:
            # the tm files may have a lamp version file that will throw when
            # pulling out a match from the regular expression. ask for
            # forgiveness and assert that it was this file that caused the
            # error.
            assert "lamp_version" in tm_file

    return service_dates
