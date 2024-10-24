import os
import pathlib

from lamp_py.performance_manager.l0_rt_vehicle_positions import (
    get_vp_dataframe,
    transform_vp_datatypes,
)
from lamp_py.performance_manager.l0_rt_trip_updates import (
    get_and_unwrap_tu_dataframe,
)
from lamp_py.performance_manager.gtfs_utils import (
    add_missing_service_dates,
    service_date_from_timestamp,
)

from ..test_resources import test_files_dir, csv_to_vp_parquet


def test_service_date_from_timestamp() -> None:
    """
    test that the service date from timestamp function correctly handles
    timestamps around the threshold when the service date switches over.
    """
    dst_expected = {
        # dst started on 8 march 2020, the clock goes from 1:59 -> 3:00
        20200307: [
            1583650200,  # 1:50 am
            1583650740,  # 1:59 am
            1583650799,  # 1:59:59 am
        ],
        20200308: [
            1583650800,  # 3:00 am
            1583651400,  # 3:10 am
        ],
        # dst ended on 1 nov 2020, the clock goes from 2:00 -> 1:00
        20201031: [
            1604209800,  # 1:50 am
            1604210340,  # 1:59 am
            1604210399,  # 1:59:59 am
            1604210400,  # 1:00 am (second time)
            1604214000,  # 2:00 am
            1604214000,  # 2:00 am
            1604217000,  # 2:50 am
            1604217540,  # 2:59 am
            1604217599,  # 2:59:59 am
        ],
        20201101: [
            1604217600,  # 3:00 am
            1604218200,  # 3:10 am
        ],
    }

    for service_date, timestamps in dst_expected.items():
        for timestamp in timestamps:
            assert service_date == service_date_from_timestamp(timestamp)


def test_vp_missing_service_date(tmp_path: pathlib.Path) -> None:
    """
    test that missing service dates in gtfs-rt vehicle position files can be
    correctly backfilled.
    """
    csv_file = os.path.join(test_files_dir, "vp_missing_start_date.csv")

    parquet_folder = tmp_path.joinpath("RT_VEHICLE_POSITIONS/year=2023/month=5/day=8/hour=11")
    parquet_folder.mkdir(parents=True)
    parquet_file = str(parquet_folder.joinpath("flat_file.parquet"))

    csv_to_vp_parquet(csv_file, parquet_file)

    events = get_vp_dataframe(to_load=[parquet_file], route_ids=["Blue"])
    events = transform_vp_datatypes(events)

    # ensure that there are NaN service dates
    assert events["service_date"].hasnans

    # add the service dates that are missing
    events = add_missing_service_dates(events, timestamp_key="vehicle_timestamp")

    # check that new service dates match existing and are numbers
    assert len(events["service_date"].unique()) == 1
    assert not events["service_date"].hasnans


def test_tu_missing_service_date() -> None:
    """
    test that trip update gtfs data with missing service dates can be processed
    correctly.
    """
    parquet_file = os.path.join(test_files_dir, "tu_missing_start_date.parquet")
    events = get_and_unwrap_tu_dataframe([parquet_file], route_ids=["Blue"])

    # check that NaN service dates exist from reading the file
    assert events["service_date"].hasnans

    events = add_missing_service_dates(events_dataframe=events, timestamp_key="timestamp")

    # check that all service dates exist and are the same
    assert not events["service_date"].hasnans
    assert len(events["service_date"].unique()) == 1
