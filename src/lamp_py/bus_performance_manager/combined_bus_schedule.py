import polars as pl

from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterSchedule


def join_tm_schedule_to_gtfs_schedule(gtfs: pl.DataFrame, tm: TransitMasterSchedule) -> pl.DataFrame:

    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    tm_schedule = tm.tm_schedule.collect().filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs["plan_trip_id"].unique())
    )
    gtfs2 = gtfs.rename({"plan_trip_id": "trip_id"})

    
    # breakpoint()

    # gtfs has extra trips that tm doesn't, but tm should not have ANY
    # scheduled trips that are not in gtfs -

    # this assumes that for a given trip, we do not hit the same stop_id more
    # than once. 
    
    # don't want to join left or asof because we want the rows that are in 
    # tm that are not in gtfs i.e. the non -revenue stops
    # these will come in via the full join
    combined_schedule = (
        gtfs2.join(tm_schedule, on=["trip_id", "stop_id"], how="full", coalesce=True)
        .join(
            tm.tm_pattern_geo_node_xref.collect(),
            on=["PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID"],
            how="left",
            coalesce=True,
        )
        # this operation fills in the nulls for the selected columns after the join- the commented out ones do not make sense to fill in
        # leaving them in as comments to make clear that this is a conscious choice
        .with_columns(
            pl.col(
                [
                    "trip_id",
                    # "stop_id"
                    # "stop_sequence",
                    "block_id",
                    "route_id",
                    "service_id",
                    "route_pattern_id",
                    "route_pattern_typicality",
                    "direction_id",
                    "direction",
                    "direction_destination",
                    # "stop_name",
                    "plan_stop_count",
                    "plan_start_time",
                    "plan_start_dt",
                    "PATTERN_ID",
                    # "plan_travel_time_seconds",
                    # "plan_route_direction_headway_seconds",
                    # "plan_direction_destination_headway_seconds"
                ]
            )
            .fill_null(strategy="forward")  # handle added non-rev stops that are at the beginning
            .fill_null(strategy="backward")  # handle added non-rev stops that are at the end
            .over(["trip_id"])
        )
        # add a column describing what data was used to form it.
        # to form the original datasets -
        # TM + JOIN = TM
        # GTFS + JOIN = GTFS
        .with_columns(
            pl.when(pl.col("stop_sequence").is_null())
            .then(pl.lit("TM"))
            .when(pl.col("timepoint_order").is_null())  # ?
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
        )
        .with_columns(
            (
                pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
                pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
                pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
                pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
                pl.col("PATTERN_ID").cast(pl.Int64).alias("pattern_id"),
            )
        )
        # explicitly define the columns that we are grabbing at the end of the operation
        .select(
            [
                "trip_id",
                "stop_id",
                "stop_sequence",
                "block_id",
                "route_id",
                "service_id",
                "route_pattern_id",
                "route_pattern_typicality",
                "direction_id",
                "direction",
                "direction_destination",
                "stop_name",
                "plan_stop_count",
                "plan_start_time",
                "plan_start_dt",
                "plan_travel_time_seconds",
                "plan_route_direction_headway_seconds",
                "plan_direction_destination_headway_seconds",
                "tm_joined",
                "timepoint_order",
                "tm_stop_sequence",
                "tm_planned_sequence_start",
                "tm_planned_sequence_end",
                "timepoint_id",
                "timepoint_abbr",
                "timepoint_name",
                "pattern_id",
            ]
        )
        .sort(["trip_id", "tm_stop_sequence", "stop_sequence"])
    )
    return combined_schedule
