from datetime import date

import polars as pl

from lamp_py.utils.gtfs_utils import gtfs_from_parquet
from lamp_py.performance_manager.gtfs_utils import start_time_to_seconds
from lamp_py.runtime_utils.process_logger import ProcessLogger


def service_ids_for_date(service_date: date) -> pl.DataFrame:
    """
    Retrieve service_id values applicable to service_date

    :param service_date: service date of requested GTFS data

    :return dataframe:
        service_id -> String
    """
    day_of_week = service_date.strftime("%A").lower()
    service_date_str = service_date.strftime("%Y%m%d")
    service_date_int = int(service_date_str)

    calendar = gtfs_from_parquet("calendar", service_date)
    service_ids = calendar.filter(
        pl.col(day_of_week).eq(True),
        pl.col("start_date") <= service_date_int,
        pl.col("end_date") >= service_date_int,
    ).select("service_id")

    calendar_dates = gtfs_from_parquet("calendar_dates", service_date)
    exclude_ids = calendar_dates.filter((pl.col("date") == service_date_int) & (pl.col("exception_type") == 2)).select(
        "service_id"
    )
    include_ids = calendar_dates.filter((pl.col("date") == service_date_int) & (pl.col("exception_type") == 1)).select(
        "service_id"
    )

    service_ids = service_ids.join(
        exclude_ids,
        on="service_id",
        how="anti",
    )

    return pl.concat([service_ids, include_ids])


def trips_for_date(service_date: date) -> pl.DataFrame:
    """
    all trip related GTFS data for a service_date

    :param service_date: service date of requested GTFS data

    :return dataframe:
        trip_id -> String
        block_id -> String
        route_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction_id -> Int8
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
            pl.col("direction_id").cast(pl.Int8),
            "direction",
            "direction_destination",
        )
    )


def canonical_stop_sequence(service_date: date) -> pl.DataFrame:
    """
    Create canonical stop sequence values for specified service date

    :param service_date: service date of requested GTFS data

    :return dataframe:
        route_id -> String
        direction_id -> Int64
        stop_id -> String
        canon_stop_sequence -> u32
    """

    canonical_trip_ids = (
        gtfs_from_parquet("route_patterns", service_date)
        .filter((pl.col("route_pattern_typicality") == 1) | (pl.col("route_pattern_typicality") == 5))
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
            pl.col("stop_sequence").rank("ordinal").over("route_id", "direction_id").alias("canon_stop_sequence"),
        )
    )


def stop_events_for_date(service_date: date) -> pl.DataFrame:
    """
    all stop event related GTFS data for a service_date

    :param service_date: service date of requested GTFS data

    :return dataframe:
        trip_id -> String
        stop_id -> String
        stop_sequence -> Int64
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
        plan_stop_count -> UInt32
        # canon_stop_sequence -> UInt32
        arrival_seconds -> Int64
        departure_seconds -> Int64
        plan_start_time -> Int64
        plan_start_dt -> Datetime
        plan_stop_departure_dt -> Datetime
    """
    trips = trips_for_date(service_date)

    stop_times = gtfs_from_parquet("stop_times", service_date).select(
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
    )

    stop_count = stop_times.group_by("trip_id").len("plan_stop_count")
    trip_start = stop_times.group_by("trip_id").agg(pl.col("arrival_time").min().alias("plan_start_time"))

    stops = gtfs_from_parquet("stops", service_date).select(
        "stop_id",
        "stop_name",
        "parent_station",
    )

    # canonical stop sequence logic currently under review
    # canon_stop_sequences = canonical_stop_sequence(service_date)

    stop_events = (
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
            trip_start,
            on="trip_id",
            how="left",
        )
        # .join(
        #     canon_stop_sequences,
        #     on=["route_pattern_id", "stop_id"],
        #     how="left",
        # )
        .with_columns(
            (pl.col("arrival_time").map_elements(start_time_to_seconds, return_dtype=pl.Int64)).alias(
                "arrival_seconds"
            ),
            (pl.col("departure_time").map_elements(start_time_to_seconds, return_dtype=pl.Int64)).alias(
                "departure_seconds"
            ),
            pl.col("plan_start_time").map_elements(start_time_to_seconds, return_dtype=pl.Int64),
            pl.col("direction_id").cast(pl.Int8),
        )
        .with_columns(
            (
                pl.datetime(service_date.year, service_date.month, service_date.day)
                + pl.duration(seconds=pl.col("plan_start_time"))
            )
            .alias("plan_start_dt")
            .dt.replace_time_zone("America/New_York"),
            (
                pl.datetime(service_date.year, service_date.month, service_date.day)
                + pl.duration(seconds=pl.col("departure_seconds"))
            )
            .alias("plan_stop_departure_dt")
            .dt.replace_time_zone("America/New_York"),
        )
        .drop(
            "arrival_time",
            "departure_time",
        )
    )

    return stop_events


def stop_event_metrics(stop_events: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate additional metrics columns for GTFS stop events

    :param stop_events: Current stop_events dataframe

    :return full stop_events dataframe with added columns:
        plan_travel_time_seconds -> i64
        plan_route_direction_headway_seconds -> i64
        plan_direction_destination_headway_seconds -> i64
    """

    # plan_travel_time_seconds - how many seconds in schedule between subsequent stops, for a trip
    stop_events = stop_events.with_columns(
        (pl.col("arrival_seconds") - pl.col("departure_seconds").shift())
        .over("trip_id", order_by="stop_sequence")
        .alias("plan_travel_time_seconds")
    )

    # plan_route_direction_headway_seconds - how many seconds in schedule between subsequent visits to a particular stop by a particular route
    stop_events = stop_events.with_columns(
        (
            (pl.col("departure_seconds") - pl.col("departure_seconds").shift()).over(
                ["stop_id", "direction_id", "route_id"],
                order_by="departure_seconds",
            )
        ).alias("plan_route_direction_headway_seconds")
    )

    # plan_direction_destination_headway_seconds - how many seconds in schedule between visits by any route to a particular stop
    stop_events = stop_events.with_columns(
        (
            (pl.col("departure_seconds") - pl.col("departure_seconds").shift()).over(
                ["stop_id", "direction_destination"],
                order_by="departure_seconds",
            )
        ).alias("plan_direction_destination_headway_seconds")
    )

    return stop_events


def bus_gtfs_schedule_events_for_date(service_date: date) -> pl.DataFrame:
    """
    Create data frame of all GTFS data needed by Bus PM app for a service_date

    :return dataframe:
        plan_trip_id -> String
        stop_id -> String
        stop_sequence -> Int64
        block_id -> String
        route_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction_id -> Int8
        direction -> String
        direction_destination -> String
        stop_name -> String
        plan_stop_count -> UInt32
        plan_start_time -> Int64
        plan_start_dt -> Datetime
        plan_travel_time_seconds -> Int64
        plan_route_direction_headway_seconds -> Int64
        plan_direction_destination_headway_seconds -> Int64
    """
    logger = ProcessLogger("bus_gtfs_schedule_events_for_date", service_date=service_date)
    logger.log_start()

    stop_events = stop_events_for_date(service_date)

    stop_events = stop_event_metrics(stop_events)

    drop_columns = [
        "arrival_seconds",
        "departure_seconds",
        "parent_station",
    ]
    stop_events = (
        stop_events.drop(drop_columns)
        .rename({"trip_id": "plan_trip_id"})
        .with_columns(
            pl.col("plan_trip_id").alias("trip_id_gtfs"),  # keep gtfs_plan_trip_id as original
            # Removes "_1", "_2" from trip_ids in GTFS so they are joinable to the TM trip_ids without these suffixes
            pl.col("plan_trip_id").str.replace(r"_\d", ""),
        )
    )

    logger.add_metadata(events_for_day=stop_events.shape[0])

    logger.log_complete()
    return stop_events
