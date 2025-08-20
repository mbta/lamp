import polars as pl

from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterSchedule
from lamp_py.runtime_utils.process_logger import ProcessLogger


# pylint: disable=R0801
def join_tm_schedule_to_gtfs_schedule(gtfs: pl.DataFrame, tm: TransitMasterSchedule) -> pl.DataFrame:
    """
    :param gtfs: gtfs schedule
    :param tm: transit master schedule - this gets filtered down immediately to just the trip_ids that are available in the gtfs schedule

    :return combined schedule
    """
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
        .with_columns(
            (
                pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
                pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
                pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
                pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
                pl.col("PATTERN_ID").cast(pl.Int64).alias("pattern_id"),
            )
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
                    "pattern_id",
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
            .when(pl.col("tm_stop_sequence").is_null())  # ?
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
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

    schedule = schedule.with_columns(
        (pl.col("stop_sequence") - pl.col("tm_stop_sequence")).alias("tm_gtfs_sequence_diff").abs(),
    )
    schedule = schedule.remove(
        pl.col("index").is_in(schedule.filter(pl.col("tm_gtfs_sequence_diff") > 2)["index"].implode())
    )

    non_rev_rows = tm_schedule.join(gtfs2, on=["trip_id", "stop_id"], how="anti", coalesce=True).height
    expected_row_diff = schedule.height - gtfs.height - non_rev_rows

    # print this out

    process_logger = ProcessLogger(
        "join_tm_schedule_to_gtfs_schedule",
        gtfs_rows=gtfs.height,
        tm_rows=tm_schedule.height,
        tm_rows_non_rev=non_rev_rows,
        expected_rows=gtfs.height + non_rev_rows,
        returned_rows=schedule.height,
    )
    process_logger.log_start()

    if expected_row_diff != 0:
        process_logger.log_failure(ValueError("Unexpected schedule rows!"))
    else:
        process_logger.log_complete()

    return schedule
