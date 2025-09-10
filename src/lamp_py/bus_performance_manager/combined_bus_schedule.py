import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterTables
from lamp_py.bus_performance_manager.events_tm import TransitMasterSchedule
from lamp_py.runtime_utils.process_logger import ProcessLogger


class CombinedSchedule(TransitMasterSchedule):
    "Union of GTFS and TransitMaster bus schedules."
    stop_sequence = dy.Int64(nullable=True, primary_key=False)
    trip_id = dy.String(nullable=False, primary_key=False)
    block_id = dy.String(nullable=True)
    service_id = dy.String(nullable=True)
    route_pattern_id = dy.String(nullable=True)
    route_pattern_typicality = dy.Int64(nullable=True)
    direction_id = dy.Int8(nullable=True)
    direction_destination = dy.String(nullable=True)
    stop_name = dy.String(nullable=True)
    plan_stop_count = dy.UInt32(nullable=True)
    plan_start_time = dy.Int64(nullable=True)
    plan_start_dt = dy.Datetime(nullable=True)
    plan_travel_time_seconds = dy.Int64(nullable=True)
    plan_route_direction_headway_seconds = dy.Int64(nullable=True)
    tm_joined = dy.String(nullable=True)
    tm_planned_sequence_start = dy.Int64(nullable=True)
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    tm_planned_sequence_end = dy.Int64(nullable=True)
    pattern_id = dy.Int64(nullable=True)
    tm_gtfs_sequence_diff = dy.Int64(nullable=True)


# pylint: disable=R0801
def join_tm_schedule_to_gtfs_schedule(gtfs: pl.DataFrame, tm: TransitMasterTables) -> dy.DataFrame[CombinedSchedule]:
    """
    Returns a schedule including GTFS stops, TransitMaster timepoints, and shuttle trips not sourced from TransitMaster.

        :param gtfs: gtfs schedule
        :param tm: transit master schedule - this gets filtered down immediately to just the trip_ids that are available in the gtfs schedule

        :return CombinedSchedule:
    """
    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    process_logger = ProcessLogger("join_tm_schedule_to_gtfs_schedule")
    process_logger.log_start()

    tm_schedule = tm.tm_schedule.collect().filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs["plan_trip_id"].unique().implode())
    )

    schedule = (
        gtfs.rename({"plan_trip_id": "trip_id"})
        .join(tm_schedule, on=["trip_id", "stop_id"], how="full", coalesce=True)
        .join(
            tm.tm_pattern_geo_node_xref.collect(),
            on=["PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID"],
            how="left",
            coalesce=True,
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
            .when(pl.col("tm_stop_sequence").is_null())
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("schedule_joined")
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
                "plan_stop_departure_dt",
                "plan_travel_time_seconds",
                "plan_route_direction_headway_seconds",
                "plan_direction_destination_headway_seconds",
                "schedule_joined",
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
    ).drop("index")

    valid, invalid = CombinedSchedule.filter(schedule, cast=True)

    process_logger.add_metadata(valid_rows=valid.height, invalidities=sum(invalid.counts().values()))

    if invalid.counts():
        process_logger.log_failure(dy.exc.ValidationError(",".join(invalid.counts().keys())))

    process_logger.log_complete()

    return valid
