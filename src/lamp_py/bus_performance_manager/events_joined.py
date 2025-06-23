from datetime import datetime

import polars as pl

from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_events_for_date
from lamp_py.runtime_utils import lamp_exception


def match_plan_trips(gtfs: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    """
    match all GTFS-RT trip_id's to a plan_trip_id from gtfs schedule

    3 matching strategies are used
    1. exact trip_id match
    2. exact match on route_id, direction_id, and first_stop of trip and then closest start_dt
    3. exact match on route_id, direction_id and then closest start_dt with the most amount of stop_id's in common on trip

    :return dataframe:
        trip_id -> String
        plan_trip_id -> String
    """
    # list of scheduled trips, resulting frame should only have 1 row per plan_trip_id
    # if multiple plan_trip_id's exist, trips with least number of stop_counts will be dropped
    schedule_trips = (
        schedule.with_columns(
            pl.col("stop_id").first().over("plan_trip_id", order_by="stop_sequence").alias("first_stop")
        )
        .group_by(["route_id", "plan_trip_id", "direction_id", "plan_start_dt", "first_stop"])
        .agg(
            pl.col("stop_id"),
            pl.col("stop_id").len().alias("stop_count"),
        )
        .sort("stop_count", descending=True)
        .unique("plan_trip_id", keep="first")
        .drop("stop_count")
    )

    # list of RT trips, resulting frame should only have 1 row per trip_id
    # if multiple trip_id's exist, trips with least number of stop_counts will be dropped
    rt_trips = (
        gtfs.with_columns(pl.col("stop_id").first().over("trip_id", order_by="stop_sequence").alias("first_stop"))
        .group_by(["route_id", "trip_id", "direction_id", "start_dt", "first_stop"])
        .agg(
            pl.col("stop_id"),
            pl.col("stop_id").len().alias("stop_count"),
        )
        .sort("stop_count", descending=True)
        .unique("trip_id", keep="first")
        .drop("stop_count")
    )

    # capture exact matches between actual and schedule trip_id's
    exact_matches = rt_trips.join(
        schedule_trips.select("plan_trip_id"),
        how="left",
        left_on="trip_id",
        right_on="plan_trip_id",
        coalesce=False,
        validate="1:1",
    )

    # asof join will match actual to schedule trips first by exact match on:
    # - route_id
    # - direction_id
    # - first_stop of trip
    # then will match to closest schedule start_dt within 1 hour of actual start_dt
    asof_matches = (
        exact_matches.filter(pl.col("plan_trip_id").is_null())
        .drop("plan_trip_id")
        .sort("start_dt")
        .join_asof(
            schedule_trips.drop(["stop_id"]).sort("plan_start_dt"),
            left_on="start_dt",
            right_on="plan_start_dt",
            by=["route_id", "direction_id", "first_stop"],
            strategy="nearest",
            tolerance="1h",
        )
    )

    # last match attempts to match trips that did not produce matches from exact or asof join
    # find all scheduled trip within 1 hour of actual drop and most overlap some stop_id's
    # sort by most number of stop_id's in common and then duration difference between start_dt
    last_matches = []
    for row in asof_matches.filter(pl.col("plan_trip_id").is_null()).iter_rows(named=True):
        plan_trip_id = None
        try:
            plan_trip_id = (
                schedule_trips.filter(
                    pl.col("route_id") == row["route_id"],
                    pl.col("direction_id") == row["direction_id"],
                    pl.Expr.abs(pl.col("plan_start_dt") - row["start_dt"]) < pl.duration(hours=1),
                    pl.col("stop_id").list.set_difference(row["stop_id"]).list.len() < pl.col("stop_id").list.len(),
                )
                .sort(
                    pl.col("stop_id").list.set_difference(row["stop_id"]).list.len(),
                    pl.Expr.abs(pl.col("plan_start_dt") - row["start_dt"]),
                )
                .get_column("plan_trip_id")[0]
            )
        except Exception as _:
            pass
        last_matches.append({"trip_id": row["trip_id"], "plan_trip_id": plan_trip_id})

    # join all sets of matches into a single dataframe
    # print("exact", exact_matches.filter(pl.col("plan_trip_id").is_not_null()).select("trip_id", "plan_trip_id"))
    # print("asof", asof_matches.filter(pl.col("plan_trip_id").is_not_null()).select("trip_id", "plan_trip_id"))
    # print("last", pl.DataFrame(last_matches))
    return_df = pl.concat(
        [
            exact_matches.filter(pl.col("plan_trip_id").is_not_null()).select("trip_id", "plan_trip_id"),
            asof_matches.filter(pl.col("plan_trip_id").is_not_null()).select("trip_id", "plan_trip_id"),
            pl.DataFrame(last_matches, schema={"trip_id": str, "plan_trip_id": str}),
        ],
        how="vertical",
        rechunk=True,
    )

    assert return_df.shape[0] == rt_trips.shape[0], "must produce trip match for every RT trip"

    return return_df


def join_schedule_to_rt(gtfs: pl.DataFrame) -> pl.DataFrame:
    """
    Join gtfs-rt records to gtfs schedule data

    join steps:
    1. match all RT trips to a plan trip.
    2. match plan trip data to RT events
    3. match plan event data to RT events

    :return added-columns:
        plan_trip_id -> String
        exact_plan_trip_match -> Bool
        block_id -> String
        service_id -> String
        route_pattern_id -> String
        route_pattern_typicality -> Int64
        direction -> String
        direction_destination -> String
        plan_stop_count -> UInt32
        plan_start_time -> Int64
        plan_start_dt -> Datetime
        stop_name -> String
        plan_travel_time_seconds -> Int64
        plan_route_direction_headway_seconds -> Int64
        plan_direction_destination_headway_seconds -> Int64
    """
    service_dates = gtfs.get_column("service_date").unique()

    if len(service_dates) == 0:
        raise lamp_exception.LampExpectedNotFoundError(f"no records for service_date found: {service_dates}")
    if len(service_dates) > 1:
        raise lamp_exception.LampInvalidProcessingError(f"more than 1 service_date found: {service_dates}")

    service_date = datetime.strptime(service_dates[0], "%Y%m%d")

    schedule = bus_gtfs_events_for_date(service_date)

    # point type requires the schedule to be known, so likely handling it here after
    # join_schedule_to_rt
    # bus_df = bus_df.with_columns(
    #     pl.coalesce(
    #         pl.when(pl.col("tm_stop_sequence") == pl.col("tm_stop_sequence").min()).then(0),
    #         pl.when(pl.col("tm_stop_sequence") == pl.col("tm_stop_sequence").max()).then(2),
    #         pl.lit(1),
    #     )
    #     .over("trip_id", "route_id", "vehicle_label")
    #     .alias("tm_point_type")
    # )

    # if for any group over, does not have a 0 and a 2 for point_type,
    # valid trip is false
    # rename valid trip to something better.

    matched_plan_trips = match_plan_trips(gtfs, schedule)

    # get a plan_trip_id from the schedule for every rt trip_id
    gtfs = gtfs.join(matched_plan_trips, on="trip_id", how="left", coalesce=True, validate="m:1").with_columns(
        pl.col("trip_id").eq(pl.col("plan_trip_id")).alias("exact_plan_trip_match")
    )

    # join plan scheudle trip data to rt gtfs
    gtfs = gtfs.join(
        (
            schedule.select(
                "plan_trip_id",
                "block_id",
                "service_id",
                "route_pattern_id",
                "route_pattern_typicality",
                "direction",
                "direction_destination",
                "plan_stop_count",
                "plan_start_time",
                "plan_start_dt",
            ).unique()
        ),
        on="plan_trip_id",
        how="left",
        coalesce=True,
        validate="m:1",
    ).join(
        (
            schedule.select(
                "stop_id",
                "stop_name",
            ).unique()
        ),
        on="stop_id",
        how="left",
        coalesce=True,
        validate="m:1",
    )

    # join plan schedule evenat data to rt gtfs
    # asof join on stop_sequence after normal join on plan_trip_id and stop_id
    # this is because the same stop_id can appear on a trip multiple times
    gtfs = gtfs.join_asof(
        schedule.select(
            "plan_trip_id",
            "stop_id",
            "stop_sequence",
            "plan_travel_time_seconds",
            "plan_route_direction_headway_seconds",
            "plan_direction_destination_headway_seconds",
        ),
        on="stop_sequence",
        by=["plan_trip_id", "stop_id"],
        strategy="nearest",
        tolerance=5,
        coalesce=True,
    )

    return gtfs


def join_tm_to_rt(gtfs: pl.DataFrame, tm: pl.DataFrame) -> pl.DataFrame:
    """
    Join gtfs-rt and transit master (tm) event dataframes

    :return added-columns:
        tm_scheduled_time_dt -> Datetime
        tm_actual_arrival_dt -> Datetime
        tm_actual_departure_dt -> Datetime
        tm_scheduled_time_sam -> Int64
        tm_actual_arrival_time_sam -> Int64
        tm_actual_departure_time_sam -> Int64
    """

    # join gtfs and tm datasets using "asof" strategy for stop_sequence columns
    # asof strategy finds nearest value match between "asof" columns if exact match is not found
    # will perform regular left join on "by" columns

    # there are frequent occasions where the stop_sequence and tm_stop_sequence are not exactly the same
    # usually off by 1 or so. By matching the nearest stop sequence
    # after grouping by trip, route, vehicle, and most importantly for sequencing - stop_id
    first_part = gtfs.sort(by="stop_sequence").join_asof(
        tm.sort("tm_stop_sequence"),
        left_on="stop_sequence",
        right_on="tm_stop_sequence",
        by=["trip_id", "route_id", "vehicle_label", "stop_id"],
        strategy="nearest",
        coalesce=True,
    )

    second_parts = []

    single_service_date = first_part.get_column("service_date").head(1)

    # this should be able to be simplified - # which stop_ids are not in GTFS in tm?
    for name, trip in first_part.group_by(["trip_id", "route_id", "vehicle_label"]):
        tm_tmp = tm.filter(
            pl.col("trip_id") == trip["trip_id"][0],
            pl.col("route_id") == trip["route_id"][0],
            pl.col("vehicle_label") == trip["vehicle_label"][0],
        )
        tm_tmp = tm_tmp.with_columns(pl.lit(False).alias("tm_joined"))

        if trip["tm_stop_sequence"].is_not_null().sum() != tm_tmp.height:
            tm_exclusive = tm_tmp.filter(
                ~pl.col("tm_stop_sequence").is_in(trip["tm_stop_sequence"].drop_nulls().unique())
            )

            trip = pl.concat([trip, tm_exclusive], how="align")
            trip = trip.with_columns(
                (pl.col("service_date").fill_null(single_service_date)),
                (pl.col("start_time").fill_null(strategy="forward")),
                (pl.col("stop_count").fill_null(strategy="forward")),
                (pl.col("direction_id").fill_null(strategy="forward")),
                (pl.col("vehicle_id").fill_null(strategy="forward")),
            )
            if len(trip.get_column("service_date").unique()) > 1:
                print("why null")
            second_parts.append(trip)
        print(len(second_parts))

    output_df = pl.concat(second_parts)
    return output_df
