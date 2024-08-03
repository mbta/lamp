import os
import shutil
from datetime import datetime

import polars as pl

from lamp_py.runtime_utils.remote_files import RemoteFileLocations
from lamp_py.aws.s3 import file_list_from_s3, download_file
from lamp_py.performance_manager.gtfs_utils import start_time_to_seconds


def sync_gtfs_files(service_date: int) -> None:
    """
    sync local tmp folder with parquet schedule data for service_date from S3

    resulting files will be located at /tmp/gtfs_archive/YYYYMMDD/...

    the /tmp/gtfs_archive/ folder will only contain one service date at a time

    :param service_date: service date of requested GTFS data 20240101 = Jan 1, 2024
    """
    gtfs_year = int(str(service_date)[:4])
    gtfs_archive_folder = os.path.join("/tmp", "gtfs_archive")
    gtfs_date_folder = os.path.join(gtfs_archive_folder, str(service_date))

    # local files already exist
    if (
        os.path.exists(gtfs_date_folder)
        and len(os.listdir(gtfs_date_folder)) > 0
    ):
        return

    # clean gtfs_archive folder
    shutil.rmtree(gtfs_archive_folder, ignore_errors=True)
    os.makedirs(gtfs_date_folder, exist_ok=True)

    s3_objects = file_list_from_s3(
        bucket_name=RemoteFileLocations.compressed_gtfs.bucket_name,
        file_prefix=os.path.join(
            RemoteFileLocations.compressed_gtfs.file_prefix, str(gtfs_year)
        ),
    )

    # check previous calendar year for s3_files, if none for current year
    # for when year just turned over, but new year files are not yet available
    # this may be fixed in the future with the compressed ingestion process
    if len(s3_objects) == 0:
        gtfs_year -= 1

        s3_objects = file_list_from_s3(
            bucket_name=RemoteFileLocations.compressed_gtfs.bucket_name,
            file_prefix=os.path.join(
                RemoteFileLocations.compressed_gtfs.file_prefix, str(gtfs_year)
            ),
        )

        if len(s3_objects) == 0:
            raise FileNotFoundError(
                f"No Compressed GTFS archive files available for {gtfs_year}"
            )

    for s3_object in s3_objects:
        if s3_object.endswith(".parquet"):
            parquet_file = s3_object.split("/")[-1]
            download_file(
                s3_object, os.path.join(gtfs_date_folder, parquet_file)
            )


def gtfs_from_parquet(file: str, service_date: int) -> pl.DataFrame:
    """
    Get GTFS data from specified file and service date

    This will read from local gtfs_archive location "tmp/gtfs_archive/YYYYMMDD/..."

    :param file: gtfs file to acces (i.e. "feed_info")
    :param service_date: service date of requested GTFS data 20240101 = Jan 1, 2024
    """
    gtfs_archive_folder = os.path.join("/tmp", "gtfs_archive")

    if not file.endswith(".parquet"):
        file = f"{file}.parquet"

    gtfs_file = os.path.join(gtfs_archive_folder, str(service_date), file)

    gtfs_df = (
        pl.read_parquet(gtfs_file)
        .filter(
            (pl.col("gtfs_active_date") <= service_date)
            & (pl.col("gtfs_end_date") >= service_date)
        )
        .drop(["gtfs_active_date", "gtfs_end_date"])
    )

    return gtfs_df


def service_ids_for_date(service_date: int) -> pl.DataFrame:
    """
    Retrieve service_id values applicable to service_date

    :param service_date: service date of requested GTFS data 20240101 = Jan 1, 2024

    :return dataframe:
        service_id -> String
    """
    service_date_dt = datetime.strptime(str(service_date), "%Y%m%d").date()
    day_of_week = service_date_dt.strftime("%A").lower()

    calendar = gtfs_from_parquet("calendar", service_date)
    service_ids = calendar.filter(
        pl.col(day_of_week)
        == True
        & (pl.col("start_date") <= service_date)
        & (pl.col("end_date") >= service_date)
    ).select("service_id")

    calendar_dates = gtfs_from_parquet("calendar_dates", service_date)
    exclude_ids = calendar_dates.filter(
        (pl.col("date") == service_date) & (pl.col("exception_type") == 2)
    ).select("service_id")
    include_ids = calendar_dates.filter(
        (pl.col("date") == service_date) & (pl.col("exception_type") == 1)
    ).select("service_id")

    service_ids = service_ids.join(
        exclude_ids,
        on="service_id",
        how="anti",
    )

    return pl.concat([service_ids, include_ids])


def trips_for_date(service_date: int) -> pl.DataFrame:
    """
    all trip related GTFS data for a service_date

    :return dataframe:
        trip_id -> String
        block_id -> String
        route_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction_id -> Int64
        direction -> String
        direction_destination -> String
    """
    service_ids = service_ids_for_date(service_date)

    # select only BUS routes
    routes = gtfs_from_parquet("routes", service_date)
    routes = routes.filter(pl.col("route_type") == 3).select("route_id")

    directions = gtfs_from_parquet("directions", service_date)

    route_patterns = gtfs_from_parquet("route_patterns", service_date).select(
        "route_pattern_id",
        "route_pattern_typicality",
    )

    trips = gtfs_from_parquet("trips", service_date)
    return (
        trips.join(
            routes,
            on="route_id",
            how="inner",
        )
        .join(
            service_ids,
            on="service_id",
            how="inner",
        )
        .join(
            directions,
            on=["route_id", "direction_id"],
            how="inner",
        )
        .join(
            route_patterns,
            on="route_pattern_id",
            how="inner",
        )
        .select(
            "trip_id",
            "block_id",
            "route_id",
            "service_id",
            "route_pattern_id",
            "route_pattern_typicality",
            "direction_id",
            "direction",
            "direction_destination",
        )
    )


def canonical_stop_sequence(service_date: int) -> pl.DataFrame:
    """
    Create canonical stop sequence values for specified service date

    :param service_date: service date of requested GTFS data 20240101 = Jan 1, 2024

    :return dataframe:
        route_id -> String
        direction_id -> Int64
        stop_id -> String
        canon_stop_sequence -> u32
    """

    canonical_trip_ids = (
        gtfs_from_parquet("route_patterns", service_date)
        .filter(
            (pl.col("route_pattern_typicality") == 1)
            | (pl.col("route_pattern_typicality") == 5)
        )
        .sort(pl.col("route_pattern_typicality"), descending=True)
        .unique(["route_id", "direction_id"], keep="first")
        .select(
            "route_id",
            "direction_id",
            "representative_trip_id",
        )
    )

    return (
        gtfs_from_parquet("stop_times", service_date)
        .join(
            canonical_trip_ids,
            left_on="trip_id",
            right_on="representative_trip_id",
            how="inner",
        )
        .select(
            "route_id",
            "direction_id",
            "stop_id",
            pl.col("stop_sequence")
            .rank("ordinal")
            .over("route_id", "direction_id")
            .alias("canon_stop_sequence"),
        )
    )


def stop_events_for_date(service_date: int) -> pl.DataFrame:
    """
    all stop event related GTFS data for a service_date

    :param service_date: service date of requested GTFS data 20240101 = Jan 1, 2024

    :return dataframe:
        trip_id -> String
        stop_id -> String
        stop_sequence -> Int64
        timepoint -> Int64
        checkpoint_id -> String
        block_id -> String
        route_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction_id -> Int64
        direction -> String
        direction_destination -> String
        stop_name -> String
        parent_station -> String
        static_stop_count -> UInt32
        canon_stop_sequence -> UInt32
        arrival_seconds -> Int64
        departure_seconds -> Int64
    """
    trips = trips_for_date(service_date)

    stop_times = gtfs_from_parquet("stop_times", service_date).select(
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "timepoint",
        "checkpoint_id",
    )

    stop_count = stop_times.group_by("trip_id").len("static_stop_count")

    stops = gtfs_from_parquet("stops", service_date).select(
        "stop_id",
        "stop_name",
        "parent_station",
    )

    canon_stop_sequences = canonical_stop_sequence(service_date)

    return (
        stop_times.join(
            trips,
            on="trip_id",
            how="inner",
        )
        .join(
            stops,
            on="stop_id",
            how="left",
        )
        .join(
            stop_count,
            on="trip_id",
            how="left",
        )
        .join(
            canon_stop_sequences,
            on=["route_id", "direction_id", "stop_id"],
            how="left",
        )
        .with_columns(
            pl.col("arrival_time")
            .map_elements(start_time_to_seconds, return_dtype=pl.Int64)
            .alias("arrival_seconds"),
            pl.col("departure_time")
            .map_elements(start_time_to_seconds, return_dtype=pl.Int64)
            .alias("departure_seconds"),
        )
        .drop(
            "arrival_time",
            "departure_time",
        )
    )


def stop_event_metrics(stop_events: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate additional metrics columns for GTFS stop events

    :param stop_events: Current stop_events dataframe

    :return full stop_events dataframe with added columns:
        plan_travel_time_seconds -> i64
        plan_route_direction_headway_seconds -> i64
        plan_direction_destination_headway_seconds -> i64
    """
    # travel times
    stop_events = stop_events.with_columns(
        (
            pl.col("arrival_seconds")
            - pl.col("departure_seconds")
            .shift()
            .over("trip_id", order_by="stop_sequence")
        ).alias("plan_travel_time_seconds")
    )

    # direction_id headway
    stop_events = stop_events.with_columns(
        (
            pl.col("departure_seconds")
            - pl.col("departure_seconds")
            .shift()
            .over(
                ["stop_id", "direction_id", "route_id"],
                order_by="departure_seconds",
            )
        ).alias("plan_route_direction_headway_seconds")
    )

    # direction_destination headway
    stop_events = stop_events.with_columns(
        (
            pl.col("departure_seconds")
            - pl.col("departure_seconds")
            .shift()
            .over(
                ["stop_id", "direction_destination"],
                order_by="departure_seconds",
            )
        ).alias("plan_direction_destination_headway_seconds")
    )

    return stop_events


def gtfs_events_for_date(service_date: int) -> pl.DataFrame:
    """
    Create data frame of all GTFS data needed by Bus PM app for a service_date

    :return dataframe:
        trip_id -> String
        stop_id -> String
        stop_sequence -> Int64
        timepoint -> Int64
        checkpoint_id -> String
        block_id -> String
        route_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction_id -> Int64
        direction -> String
        direction_destination -> String
        stop_name -> String
        parent_station -> String
        static_stop_count -> UInt32
        canon_stop_sequence -> UInt32
        arrival_seconds -> Int64
        departure_seconds -> Int64
        plan_travel_time_seconds -> Int64
        plan_route_direction_headway_seconds -> Int64
        plan_direction_destination_headway_seconds -> Int64
    """
    sync_gtfs_files(service_date)

    stop_events = stop_events_for_date(service_date)

    stop_events = stop_event_metrics(stop_events)

    return stop_events
