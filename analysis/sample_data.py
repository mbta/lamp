# type: ignore
# sample data and save as csv for testing

from typing import List
import polars as pl
import pyarrow
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc

from pyarrow.fs import S3FileSystem
import os
from lamp_py.runtime_utils.remote_files import (
    glides_trips_updated,
    glides_operator_signed_in,
    tableau_glides_all_trips_updated,
    tableau_glides_all_operator_signed_in,
)
from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_with_details, object_exists
from lamp_py.ingestion.utils import group_sort_file_list
import datetime

# def


def sample_dataset(parquet_path: List[str], output_csv_base_path: str) -> None:
    """
    WIP - sample a dataset and save off as CSV to create commitable test data.
    """
    for pq_path in parquet_path:
        print(pq_path)
        ds = pd.dataset(pq_path)

        out_csv_fname = os.path.basename(pq_path).replace(".parquet", ".csv")
        batch = ds.to_batches(batch_size=1000, batch_readahead=0, fragment_readahead=0)
        first_batch = next(iter(batch))
        pl_batch = pl.from_arrow(first_batch)
        pl_batch.write_csv(os.path.join(output_csv_base_path, out_csv_fname))


# # given list of parquet objects
# # grab each one of them
# # load each by batch (to not blow up) (~2000 lines)
# # grab the first 1000 lines ( as pq/pl )
# # convert to csv
# # convert back to parquet
# # assert (1000 == 1000)
# # save object in output path


# # make a test for each file type - paramaterized input to each test
# import pytest

# @pytest.mark.parametrize("input, expected", [
#     (2, 4),
#     (3, 9),
#     (4, 16),
# ])
# def test_square(input, expected):
#     assert input * input == expected

# # form s3 list
# # bucket_name = "mbta-ctd-dataplatform-archive"
# # # # bucket_name = ""
# # # bucket_name = "mbta-ctd-dataplatform-dev-incoming"
# # # bucket_name = "mbta-ctd-dataplatform-dev-incoming"
# # # s3://mbta-ctd-dataplatform-archive/lamp/delta/2025/04/09/2025-04-09T00:00:00Z_https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz

# # # file_prefix = "lamp/delta/2025/03/31/2025-03-31T00:00:"
# # file_prefix = "lamp/delta/2025/04/09"
# # # in_filter="https_cdn.mbta.com_realtime_TripUpdates_enhanced")
# # # https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
# # # https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
# # # https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
# # # https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
# # # https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
# # # https_mbta_integration.mybluemix.net_vehicleCount.gz

# # #s3://mbta-ctd-dataplatform-dev-incoming/lamp/delta/2025/03/31/2025-03-31T00:00:03Z_s3_mbta_ctd_trc_data_rtr_prod_LightRailRawGPS.json.gz
# # # file_prefix = os.path.join("lamp", "RT_VEHICLE_POSITIONS/year=2025/month=1/")
# # # max_list_size: int = 250_000,
# # # in_filter: Optional[str] = None

# # files = file_list_from_s3(bucket_name=bucket_name, file_prefix=file_prefix, max_list_size=1_000_000)

# filexx = pq.read_table("s3://mbta-ctd-dataplatform-dev-archive/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_HR_30_day.parquet")
# f4 = pl.from_arrow(filexx)


# def load_trip_updates(path):
#     file = pq.read_table(path)
#     f1 = pl.from_arrow(file)
#     return f1


# prod_tu = load_trip_updates("s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_LR_60_day.parquet")

# dev_tu = load_trip_updates("s3://mbta-ctd-dataplatform-dev-archive/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_LR_60_day.parquet")

# staging_tu = load_trip_updates("s3://mbta-ctd-dataplatform-staging-archive/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_LR_60_day.parquet")


# ds = pd.dataset('s3://mbta-ctd-dataplatform-springboard/lamp/DEV_GREEN_RT_TRIP_UPDATES/year=2025/month=4/day=27/2025-04-27T00:00:00.parquet')

# batch = ds.to_batches()
# tu = pl.from_arrow(batch)

# dev_prod_tu['trip_update.timestamp']

# assert(f1['trip_update.trip.route_id'].unique().is_in(["Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]).all())

# assert(f1['trip_update.stop_time_update.stop_id'].unique().is_in(["70106", "70160", "70161", "70238", "70276", "70503", "70504", "70511", "70512"]).all())


# # check 60 days in past. check not past 60 days
# from datetime import datetime

# f1['feed_timestamp'][0] - pl.lit(datetime.now())


# f1.with_columns(
#    pl.when(
#        pl.col("feed_timestamp").dt.month().is_in([1, 3])
#    )
#    .then(pl.lit("Yes"))
#    .otherwise(pl.lit("No"))
#    .alias("match")
# )


if __name__ == "__main__":
    # grab all the tableau outputs in the prod directories - remove the rail and alerts for now
    # this is sampling_list (below)

    # list = file_list_from_s3(bucket_name="mbta-ctd-dataplatform-archive", file_prefix="lamp/tableau")
    # list2 = file_list_from_s3(bucket_name="mbta-performance", file_prefix="lamp/tableau")
    # list.extend(list2)
    # print(list)

    sampling_list = [
        "s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_TripUpdates_LR_60_day.parquet",
        "s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_VehiclePositions_LR_60_day.parquet",
        "s3://mbta-ctd-dataplatform-archive/lamp/tableau/glides/LAMP_ALL_Glides_operator_sign_ins.parquet",
        "s3://mbta-ctd-dataplatform-archive/lamp/tableau/glides/LAMP_ALL_Glides_trip_updates.parquet",
        "s3://mbta-performance/lamp/tableau/bus/LAMP_ALL_Bus_Events.parquet",
        "s3://mbta-performance/lamp/tableau/bus/LAMP_RECENT_Bus_Events.parquet",
        "s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_HR_30_day.parquet",
        "s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_TripUpdates_LR_60_day.parquet",
        "s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_VehiclePositions_HR_30_day.parquet",
        "s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_VehiclePositions_LR_60_day.parquet",
    ]

    # sample_dataset(sampling_list, '/Users/hhuang/lamp/lamp/tests/test_files/ARCHIVE')
    # for pq_path in parquet_path:
    #         print(pq_path)
    #         ds =

    #         out_csv_fname = os.path.basename(pq_path).replace('.parquet', '.csv')
    #         batch = ds.to_batches(batch_size=1000, batch_readahead=0, fragment_readahead=0)
    #         first_batch = next(iter(batch))
    #         pl_batch = pl.from_arrow(first_batch)
    #         pl_batch.write_csv(os.path.join(output_csv_base_path, out_csv_fname))
    #         breakpoint()
    # ds = pd.dataset("s3://mbta-performance/lamp/tableau/rail/LAMP_ALL_RT_fields.parquet")
    # ds = pd.dataset("s3://mbta-ctd-dataplatform-staging-archive/lamp/tableau/rail/LAMP_ALL_RT_fields.parquet")
    # # dates = []
    # # for bat in rail.to_batches(batch_size=500_000, batch_readahead=1, fragment_readahead=0, columns=['service_date']):
    # dates = []
    # # todo - 30 days? 31 days?
    # for day in range(1, 31):
    #     date = datetime.datetime(2025, 4, day)
    #     for bat in ds.to_batches(
    #         batch_size=500_000, batch_readahead=5, fragment_readahead=0, columns=["service_date", "route_id"]
    #     ):
    #         # breakpoint()
    #         pls = pl.from_arrow(bat)
    #         res = pls.filter(pl.col("service_date") == date)
    #         # breakpoint()
    #         if res.height > 0:
    #             dates.append(date)
    #             print(f"ok: {date}, {res.height}")
    #         # print(".")÷

    # assert(all([(dates[i+1] - dates[i]).days < 2 for i in range(len(dates)-1)]))
    # breakpoint()

    # check devgreen light rail
    # check light rail
    # check heavy rail
    # check glides
    # check bus
