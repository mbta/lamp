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


def bus_routes_for_service_date(service_date: date) -> List[str]:
    """get a list of bus route ids for a given service date"""
    routes_file = get_gtfs_parquet_file(
        year=service_date.year, filename="routes.parquet"
    ).get_s3_path()

    # generate an integer date for the service date
    target_date = int(service_date.strftime("%Y%m%d"))

    bus_routes = (
        pl.scan_parquet(routes_file)
        .select(["route_id", "route_type", "gtfs_active_date", "gtfs_end_date"])
        .filter(
            (pl.col("gtfs_active_date") < target_date)
            & (pl.col("gtfs_end_date") > target_date)
            & (pl.col("route_type") == 3)
        )
        .collect()
    )

    return bus_routes["route_id"].unique().to_list()


def generate_gtfs_rt_events(
    service_date: date, gtfs_rt_files: List[str]
) -> pl.DataFrame:
    """
    generate a polars dataframe for bus vehicle events from gtfs realtime
    vehicle position files for a given service date
    """
    bus_routes = bus_routes_for_service_date(service_date)

    # build a dataframe of every gtfs record
    # * scan all of the gtfs realtime files
    # * only pull out bus records and records for the service date
    # * rename / convert columns as appropriate
    # * sort by vehicle id and timestamp
    # * keep only the first record for a given trip / stop / status
    # * sort again by vehicle id and timestamp (the unique call seems to reorder)
    vehicle_positions = (
        pl.scan_parquet(gtfs_rt_files)
        .filter(
            (pl.col("vehicle.trip.route_id").is_in(bus_routes))
            & (
                pl.col("vehicle.trip.start_date")
                == service_date.strftime("%Y%m%d")
            )
        )
        .select(
            pl.col("vehicle.trip.route_id").cast(pl.String).alias("route_id"),
            pl.col("vehicle.trip.trip_id").cast(pl.String).alias("trip_id"),
            pl.col("vehicle.stop_id").cast(pl.String).alias("stop_id"),
            pl.col("vehicle.trip.direction_id").alias("direction_id"),
            pl.col("vehicle.trip.start_time").alias("start_time"),
            pl.col("vehicle.trip.start_date").alias("service_date"),
            pl.col("vehicle.vehicle.id").cast(pl.String).alias("vehicle_id"),
            pl.col("vehicle.current_status").alias("current_status"),
            pl.from_epoch("vehicle.timestamp")
            .dt.convert_time_zone("EST5EDT")
            .alias("vehicle_timestamp"),
        )
        .unique()
        .sort(["vehicle_id", "vehicle_timestamp"])
        .unique(
            subset=[
                "route_id",
                "trip_id",
                "stop_id",
                "direction_id",
                "vehicle_id",
                "current_status",
            ],
            keep="first",
        )
        .sort(["vehicle_id", "vehicle_timestamp"])
        .collect()
    )

    # using the vehicle positions dataframe, create a dataframe for each event
    # by pivoting and mapping the current status onto arrivals and departures.
    # initially, they map to arrival times and traveling towards times. shift
    # the traveling towards datetime over trip id to get the departure time.
    vehicle_events = (
        vehicle_positions.pivot(
            values="vehicle_timestamp",
            index=[
                "route_id",
                "trip_id",
                "stop_id",
                "start_time",
                "service_date",
                "vehicle_id",
            ],
            columns="current_status",
        )
        .rename(
            {
                "STOPPED_AT": "arrival_gtfs",
                "IN_TRANSIT_TO": "started_towards_datetime",
            }
        )
        .with_columns(
            pl.col("started_towards_datetime")
            .shift(-1)
            .over("trip_id")
            .alias("departure_gtfs")
        )
    )

    return vehicle_events
