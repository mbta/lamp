import polars as pl


def join_gtfs_tm_events(gtfs: pl.DataFrame, tm: pl.DataFrame) -> pl.DataFrame:
    """
    Join gtfs-rt and transit master (tm) event dataframes

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> String
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> String
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime
        gtfs_arrival_dt -> Datetime
        tm_stop_sequence -> Int64
        tm_is_layover -> Bool
        tm_arrival_dt -> Datetime
        tm_departure_dt -> Datetime
        gtfs_sort_dt -> Datetime
        gtfs_depart_dt -> Datetime
    """

    # join gtfs and tm datasets using "asof" strategy for stop_sequence columns
    # asof strategy finds nearest value match between "asof" columns if exact match is not found
    # will perform regular left join on "by" columns

    return (
        gtfs.sort(by="stop_sequence")
        .join_asof(
            tm.sort("tm_stop_sequence"),
            left_on="stop_sequence",
            right_on="tm_stop_sequence",
            by=["trip_id", "route_id", "vehicle_label", "stop_id"],
            strategy="nearest",
            coalesce=True,
        )
        .with_columns(
            (
                pl.coalesce(
                    ["gtfs_travel_to_dt", "gtfs_arrival_dt"],
                ).alias("gtfs_sort_dt")
            )
        )
        .with_columns(
            (
                pl.col("gtfs_travel_to_dt")
                .shift(-1)
                .over(
                    ["vehicle_label", "trip_id"],
                    order_by="gtfs_sort_dt",
                )
                .alias("gtfs_depart_dt")
            )
        )
    )
