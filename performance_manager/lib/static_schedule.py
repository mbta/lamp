import logging
import os
import pyarrow.parquet as pq
from pyarrow import fs

from .s3_utils import file_list_from_s3, read_parquet


def has_been_processed(timestamp: str) -> bool:
    """
    this should check against the metadata table to see if this timestamp needs
    to be processed still

    for now just use the latest one
    """
    return timestamp != "1659568605"


def process_all_static_schedules():
    for filename in file_list_from_s3(
        bucket_name=os.environ["EXPORT_BUCKET"], file_prefix="lamp/STOP_TIMES/"
    ):

        # there are some old prefixes in there that we want to get rid of, skip
        # them for now
        if "timestamp" not in filename:
            continue

        # current impl also gives "directories", not just full files
        if "parquet" not in filename:
            continue

        for piece in filename.split("/"):
            if "timestamp" in piece:
                _, timestamp = piece.split("=")

        if not has_been_processed(timestamp):
            return process_static_schedule(timestamp)


def get_filepath(filetype: str, timestamp: str) -> list[str]:
    for filename in file_list_from_s3(
        bucket_name=os.environ["EXPORT_BUCKET"],
        file_prefix=f"lamp/{filetype}/timestamp={timestamp}/",
    ):
        if "parquet" not in filename:
            continue
        return filename


def process_static_schedule(timestamp: str):
    logging.info("Reading Static Info for Timestamp %s", timestamp)
    trip_filepath = get_filepath("TRIPS", timestamp)
    trips = read_parquet(trip_filepath)

    routes_filepath = get_filepath("ROUTES", timestamp)
    routes = read_parquet(routes_filepath)

    gtfs_stops_filepath = get_filepath("STOPS", timestamp)
    gtfs_stops = read_parquet(gtfs_stops_filepath)

    stop_times_filepath = get_filepath("STOP_TIMES", timestamp)
    stop_times = read_parquet(stop_times_filepath)

    calendar_filepath = get_filepath("CALENDAR", timestamp)
    calendar = read_parquet(calendar_filepath)

    feed_info_filepath = get_filepath("FEED_INFO", timestamp)
    feed_info = read_parquet(feed_info_filepath)

    logging.info("Completed Reading Static Info")

    # Create stop_id lookup table. Converts all stop_id's into 'parent_station'
    # values. 'parent_station' comes from gtfs 'stops' table. 'parent_station'
    # is used for every stop_id with a 'parent_station' value. Otherwise the
    # stop_id is assumed to be a parent station.

    stop_id_lookup = gtfs_stops.loc[:, ["stop_id", "parent_station"]]
    mask = stop_id_lookup.parent_station.isna()
    stop_id_lookup.loc[mask, "parent_station"] = stop_id_lookup.loc[
        mask, "stop_id"
    ]

    # Calculate expected headways from GTFS static data.
    #
    # Requires joining of stop_times with routes, trips, stop_id_lookup and
    # calendar tables.
    """
    Drop un-used columns from stop_times
    """
    stop_times_drop_columns = [
        "stop_headsign",
        "continuous_pickup",
        "continuous_drop_off",
    ]
    gtfs_headways = stop_times.drop(columns=stop_times_drop_columns)

    """
    Merge tables
    """
    t_trips = trips.merge(
        routes.loc[:, ["route_id", "route_type"]], how="left", on=["route_id"]
    )
    gtfs_headways = gtfs_headways.merge(
        t_trips.loc[
            :,
            ["trip_id", "route_type", "route_id", "service_id", "direction_id"],
        ],
        how="left",
        on=["trip_id"],
    )
    gtfs_headways = gtfs_headways.merge(
        stop_id_lookup.loc[:, :], how="left", on=["stop_id"]
    )
    gtfs_headways = gtfs_headways.merge(
        calendar.loc[:, ["service_id", "thursday"]],
        how="left",
        on=["service_id"],
    )

    """
    limit headways to subway lines (route_type less than 2)
    """
    sub_gtfs_headways = gtfs_headways.loc[(gtfs_headways.route_type < 2), :]

    """
    Function to convert departure_time to all seconds departure_time_sec
    """

    def time_to_seconds(time: str) -> int:
        (hour, min, sec) = time.split(":")
        return int(hour) * 3600 + int(min) * 60 + int(sec)

    sub_gtfs_headways = sub_gtfs_headways.assign(
        departure_time_sec=sub_gtfs_headways["departure_time"].apply(
            time_to_seconds
        )
    )

    """
    Sort stop_times to group data of same lines for headways calculation
    """
    sub_gtfs_headways = sub_gtfs_headways.sort_values(
        by=[
            "direction_id",
            "route_id",
            "service_id",
            "parent_station",
            "departure_time_sec",
        ]
    )

    """
    Calculate previous stop departure time with shift on sorted data.
    """
    sub_gtfs_headways = sub_gtfs_headways.assign(
        prev_departure_time_sec=sub_gtfs_headways["departure_time_sec"]
        .shift()
        .where(
            sub_gtfs_headways.parent_station.eq(
                sub_gtfs_headways.parent_station.shift()
            )
        )
        .astype("int", errors="ignore")
    )

    """
    Where prev_departure_time_sec is NA, shift occured between different stations, so head_way calculation would not be valid.
    Drop these rows
    Essentially first stop of each route/service combination
    """
    sub_gtfs_headways.dropna(
        axis=0, subset=["prev_departure_time_sec"], inplace=True
    )

    """
    Calculate headway as departure_time_sec - prev_departure_time_sec
    """
    sub_gtfs_headways = sub_gtfs_headways.assign(
        head_way=sub_gtfs_headways["departure_time_sec"]
        - sub_gtfs_headways["prev_departure_time_sec"]
    )

    logging.info(sub_gtfs_headways.max())
    logging.info(len(sub_gtfs_headways))

    return sub_gtfs_headways
