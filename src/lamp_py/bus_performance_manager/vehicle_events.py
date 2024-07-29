from datetime import timedelta, date
from typing import Optional, Dict, List
import re

import polars as pl

from lamp_py.aws.s3 import (
    file_list_from_s3_with_details,
    get_datetime_from_partition_path,
    get_last_modified_object,
)

from lamp_py.runtime_utils.remote_files import (
    RemoteFileLocations,
    get_gtfs_parquet_file,
)


def get_new_event_files() -> List[Dict[str, date | List[str]]]:
    """
    Generate a dataframe that contains a record for every service date to be
    processed.
    * Collect all of the potential input filepaths, their last modified
        timestamp, and potential service dates.
    * Get the last modified timestamp for the output filepaths.
    * Generate a list of all service dates where the input files have been
        modified since the last output file write.
    * For each service date, generate a list of input files associated with
        that service date.

    @return pl.DataFrame -
        'service_date': datetime.date
        'gtfs_rt': list[str] - s3 filepaths for vehicle position files
        'transit_master': list[str] - s3 filepath for tm files
    """

    def get_service_date_from_filename(tm_filename: str) -> Optional[date]:
        """pull the service date from a filename formatted '1YYYYMMDD.parquet'"""
        try:
            service_date_int = re.findall(r"(\d{8}).parquet", tm_filename)[0]
            year = int(service_date_int[:4])
            month = int(service_date_int[4:6])
            day = int(service_date_int[6:])

            return date(year=year, month=month, day=day)
        except IndexError:
            # the tm files may have a lamp version file that will throw when
            # pulling out a match from the regular expression. ask for
            # forgiveness and assert that it was this file that caused the
            # error.
            assert "lamp_version" in tm_filename
            return None

    # pull all of the vehicle position files from s3 along with their last
    # modified datetime. convert to a dataframe and generate a service date
    # from the partition paths. add a source column for later merging.
    vp_objects = file_list_from_s3_with_details(
        bucket_name=RemoteFileLocations.vehicle_positions.bucket_name,
        file_prefix=RemoteFileLocations.vehicle_positions.file_prefix,
    )
    vp_df = pl.DataFrame(vp_objects).with_columns(
        pl.col("s3_obj_path")
        .apply(lambda x: get_datetime_from_partition_path(x).date())
        .alias("service_date"),
        pl.lit("gtfs_rt").alias("source"),
    )

    # the partition paths record the UTC time that the vehicle positions were
    # recorded. further, a service date will have trips with events that occur
    # on the following calendar date. generate two new dataframes from the
    # vehicle position dataframe.
    #
    # the first has records for all objects that will have potentially have
    # data from the service date one day before the calendar date. this
    # includes hourly partitioned files where the hour is leq 8 and all daily
    # partitioned files.
    #
    # the second has records for all objects that will potentially have data
    # from the service date matching the calendar date. this is all files other
    # than hourly partitioned files where the hour is leq 2.
    #
    # after generating these dataframes, concat them to create the new vehicle
    # positions dataframe containing all file / service date pairs.

    # contain data from the previous service date
    vp_shifted = vp_df.filter(
        pl.col("s3_obj_path").str.contains("hour=0")
        | pl.col("s3_obj_path").str.contains("hour=1")
        | pl.col("s3_obj_path").str.contains("hour=2")
        | pl.col("s3_obj_path").str.contains("hour=3")
        | pl.col("s3_obj_path").str.contains("hour=4")
        | pl.col("s3_obj_path").str.contains("hour=5")
        | pl.col("s3_obj_path").str.contains("hour=6")
        | pl.col("s3_obj_path").str.contains("hour=7")
        | pl.col("s3_obj_path").str.contains("hour=8")
        | ~pl.col("s3_obj_path").str.contains("hour")
    ).with_columns(
        (pl.col("service_date") - timedelta(days=1)).alias("service_date")
    )

    # these files contain data from the current service day
    vp_unshifted = vp_df.filter(
        ~pl.col("s3_obj_path").str.contains("hour=0")
        & ~pl.col("s3_obj_path").str.contains("hour=1")
        & ~pl.col("s3_obj_path").str.contains("hour=2")
    )

    # merge the shifted and unshifted dataframes
    vp_df = pl.concat([vp_unshifted, vp_shifted])

    # pull all of the transit master files from s3 along with their last
    # modified datetime. convert to a dataframe and generate a service date
    # from the filename. add a source column for later merging.
    tm_objects = file_list_from_s3_with_details(
        bucket_name=RemoteFileLocations.tm_stop_crossing.bucket_name,
        file_prefix=RemoteFileLocations.tm_stop_crossing.file_prefix,
    )
    tm_df = pl.DataFrame(tm_objects).with_columns(
        pl.col("s3_obj_path")
        .apply(get_service_date_from_filename)
        .alias("service_date"),
        pl.lit("transit_master").alias("source"),
    )

    # a merged dataframe of all files to operate on
    all_files = pl.concat([vp_df, tm_df])

    # get the last modified object in the output file s3 location
    latest_event_file = get_last_modified_object(
        bucket_name=RemoteFileLocations.bus_events.bucket_name,
        file_prefix=RemoteFileLocations.bus_events.file_prefix,
        version="1.0",
    )

    # if there is a event file, pull the service date from it and filter
    # all_files to only contain objects with service dates on or after this
    # date.
    if latest_event_file:
        latest_service_date = get_service_date_from_filename(
            latest_event_file["s3_obj_path"]
        )
        all_files = all_files.filter(
            pl.col("service_date") >= latest_service_date
        )

    # filter all files to only files associated with new service dates
    # aggregate records by source and pivot on service date.
    #
    # each record of the new dataframe will have a list of gtfs_rt and tm input
    # files.
    grouped_files = (
        all_files.groupby(["service_date", "source"])
        .agg([pl.col("s3_obj_path").alias("file_list")])
        .pivot(values="file_list", index="service_date", columns="source")
    )

    return grouped_files.to_dicts()

