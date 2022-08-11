import logging
import os

import pandas

from .s3_utils import file_list_from_s3, read_parquet


def get_dataframe(filetype: str, timestamp: str) -> pandas.core.frame.DataFrame:
    """
    get a dataframe for a static schedule filetype with a timestamp
    """
    # assuming that there is only one file in these full prefixes.
    for filepath in file_list_from_s3(
        bucket_name=os.environ["EXPORT_BUCKET"],
        file_prefix=f"lamp/{filetype}/timestamp={timestamp}/",
    ):
        return read_parquet(filepath)


def process_static_schedule(timestamp: str) -> pandas.core.frame.DataFrame:
    """
    Calculate expected headways from GTFS static data.

    Requires joining of stop_times with routes, trips, stop_id_lookup and
    calendar tables.
    """
    logging.info("Reading Static Info for Timestamp %s", timestamp)
    trips = get_dataframe("TRIPS", timestamp)
    routes = get_dataframe("ROUTES", timestamp)
    gtfs_stops = get_dataframe("STOPS", timestamp)
    stop_times = get_dataframe("STOP_TIMES", timestamp)
    calendar = get_dataframe("CALENDAR", timestamp)
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

    # Drop un-used columns from stop_times
    stop_times_drop_columns = [
        "stop_headsign",
        "continuous_pickup",
        "continuous_drop_off",
    ]
    gtfs_headways = stop_times.drop(columns=stop_times_drop_columns)

    # Merge tables
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

    # limit headways to subway lines (route_type less than 2)
    sub_gtfs_headways = gtfs_headways.loc[(gtfs_headways.route_type < 2), :]

    def time_to_seconds(time: str) -> int:
        """
        Function to convert departure_time to all seconds departure_time_sec
        """
        (hour, minute, second) = time.split(":")
        return int(hour) * 3600 + int(minute) * 60 + int(second)

    sub_gtfs_headways = sub_gtfs_headways.assign(
        departure_time_sec=sub_gtfs_headways["departure_time"].apply(
            time_to_seconds
        )
    )

    # Sort stop_times to group data of same lines for headways calculation
    sub_gtfs_headways = sub_gtfs_headways.sort_values(
        by=[
            "direction_id",
            "route_id",
            "service_id",
            "parent_station",
            "departure_time_sec",
        ]
    )

    # Calculate previous stop departure time with shift on sorted data.
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

    # Where prev_departure_time_sec is NA, shift occured between different
    # stations, so head_way calculation would not be valid.
    # Drop these rows
    # Essentially first stop of each route/service combination
    sub_gtfs_headways.dropna(
        axis=0, subset=["prev_departure_time_sec"], inplace=True
    )

    # Calculate headway as departure_time_sec - prev_departure_time_sec
    sub_gtfs_headways = sub_gtfs_headways.assign(
        head_way=sub_gtfs_headways["departure_time_sec"]
        - sub_gtfs_headways["prev_departure_time_sec"]
    )

    logging.info(sub_gtfs_headways.max())
    logging.info(len(sub_gtfs_headways))

    return sub_gtfs_headways
