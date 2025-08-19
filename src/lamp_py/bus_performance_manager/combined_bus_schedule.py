import polars as pl

from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterSchedule


def join_tm_schedule_to_gtfs_schedule(gtfs: pl.DataFrame, tm: TransitMasterSchedule) -> pl.DataFrame:

    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    tm_schedule = tm.tm_schedule.collect().filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs["plan_trip_id"].unique().implode())
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
    schedule = (
        gtfs2.join(tm_schedule, on=["trip_id", "stop_id"], how="full", coalesce=True)
        .join(
            tm.tm_pattern_geo_node_xref.collect(),
            on=["PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID"],
            how="left",
            coalesce=True,
            # validate="1:1" # this won't validate 1 to 1 because some trips stop at the same stop multiple times. correcting this below
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
    ).with_row_index()

    # add logic to remove duplicated rows from the full join. this is necessary to deduplicate for trips that stop at the same stop_id more than once.
    # it is not possible to join_asof becuase gtfs is on the left to get shuttles and non TM trips, and there is no guarantee that the sequences joined
    # will be valid given that there are additional unjoined non-rev stop sequences that would not match.
    # schedule = schedule0.filter(pl.col.trip_id == "70040149")
    # Limitation: Route 238 looks like an outlier where this processing is not successfully being handles by this logic

    # we are unable to use unique(subset=) due to the mis

    # this is an example of one of the remaining cases that are not handled correctly by the below logic. The hypothesis is that the non-deterministic sorting of 
    # stop_sequence and tm_stop_sequence is causing the "diff" values calculated to be assigned to one row or another, which is not being properly handled
    # by this logic

    # schedule.filter(pl.col.trip_id == "70040149").sort("tm_stop_sequence").filter(pl.col.stop_sequence.is_in([33,36])).
    # select(["stop_sequence", "tm_stop_sequence", "sequence_diff", "tm_sequence_diff", "tm_gtfs_sequence_diff"])
    # shape: (4, 5)
    # ┌───────────────┬──────────────────┬───────────────┬──────────────────┬───────────────────────┐
    # │ stop_sequence ┆ tm_stop_sequence ┆ sequence_diff ┆ tm_sequence_diff ┆ tm_gtfs_sequence_diff │
    # │ ---           ┆ ---              ┆ ---           ┆ ---              ┆ ---                   │
    # │ i64           ┆ i64              ┆ i64           ┆ i64              ┆ i64                   │
    # ╞═══════════════╪══════════════════╪═══════════════╪══════════════════╪═══════════════════════╡
    # │ 33            ┆ 33               ┆ 0             ┆ 0                ┆ 0                     │
    # │ 36            ┆ 33               ┆ 0             ┆ 1                ┆ 3                     │
    # │ 33            ┆ 36               ┆ 1             ┆ 0                ┆ 3                     │
    # │ 36            ┆ 36               ┆ 1             ┆ 1                ┆ 0                     │
    # └───────────────┴──────────────────┴───────────────┴──────────────────┴───────────────────────┘

    filter_out_gtfs = (pl.col.tm_sequence_diff < 1) & (pl.col("tm_gtfs_sequence_diff").ne(0)
    ).over(["trip_id", "stop_sequence"])    
    filter_out_tm = (pl.col.sequence_diff < 1)  & (pl.col("tm_gtfs_sequence_diff").ne(0)
    ).over(["trip_id", "stop_sequence"])    

    
    schedule = schedule.sort(["trip_id", "stop_sequence"]).with_columns(
        (pl.col("stop_sequence").shift(-1) - pl.col("stop_sequence")).alias("sequence_diff").abs().over("trip_id"),
        (pl.col("stop_sequence") - pl.col("tm_stop_sequence")).alias("tm_gtfs_sequence_diff").abs(),
    )
    schedule = schedule.sort(["trip_id", "tm_stop_sequence"]).with_columns(
        (pl.col("tm_stop_sequence").shift(-1) - pl.col("tm_stop_sequence")).alias("tm_sequence_diff").over("trip_id"),
    )

    filter_out = schedule.filter(filter_out_gtfs | filter_out_tm)["index"].implode()

    schedule2 = schedule.filter(~pl.col("index").is_in(filter_out)).drop()
    schedule2 = schedule2.drop(["index", "sequence_diff", "tm_sequence_diff", "tm_gtfs_sequence_diff"]).sort(
        ["trip_id", "tm_stop_sequence", "stop_sequence"]
    )

    return schedule2
