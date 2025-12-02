import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_gtfs_rt import remove_rare_variant_route_suffix
from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterTables
from lamp_py.bus_performance_manager.events_tm import TransitMasterSchedule
from lamp_py.runtime_utils.process_logger import ProcessLogger


class CombinedSchedule(TransitMasterSchedule):
    "Union of GTFS and TransitMaster bus schedules."
    checkpoint_id = dy.String(nullable=True)
    gtfs_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    stop_sequence = dy.UInt32(primary_key=True, min=1)
    block_id = dy.String(nullable=True)
    service_id = dy.String(nullable=True)
    route_pattern_id = dy.String(nullable=True)
    route_pattern_typicality = dy.Int64(nullable=True)
    direction_id = dy.Int8(nullable=True)
    direction = dy.String(nullable=True)
    direction_destination = dy.String(nullable=True)
    stop_name = dy.String(nullable=True)
    plan_stop_count = dy.UInt32(nullable=True)
    plan_start_time = dy.Int64(nullable=True)
    plan_start_dt = dy.Datetime(nullable=True, time_zone=None)  # America/New_York
    plan_stop_departure_dt = dy.Datetime(primary_key=True, time_zone=None)  # America/New_York
    tm_scheduled_time_sam = dy.Int64(nullable=True)
    plan_travel_time_seconds = dy.Int64(nullable=True)
    plan_route_direction_headway_seconds = dy.Int64(nullable=True)
    plan_direction_destination_headway_seconds = dy.Int64(nullable=True)
    schedule_joined = dy.String(nullable=False)
    tm_planned_sequence_start = dy.Int64(nullable=True)
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    tm_planned_sequence_end = dy.Int64(nullable=True)
    pattern_id = dy.Int64(nullable=True)
    point_type = dy.String(nullable=True)


# pylint: disable=R0801
def join_tm_schedule_to_gtfs_schedule(
    gtfs: pl.DataFrame, tm: TransitMasterTables, **debug_flags: dict[str, bool]
) -> dy.DataFrame[CombinedSchedule]:
    """
    Returns a schedule including GTFS stops, TransitMaster timepoints, and shuttle trips not sourced from TransitMaster.

        :param gtfs: gtfs schedule
        :param tm: transit master schedule - this gets filtered down immediately to just the trip_ids that are available in the gtfs schedule

        :return CombinedSchedule:
    """
    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    process_logger = ProcessLogger("join_tm_schedule_to_gtfs_schedule")
    process_logger.log_start()

    if debug_flags.get("write_intermediates"):
        tm.tm_schedule.sink_parquet("/tmp/tm_schedule.parquet")

    # gtfs_schedule: contains _1, _2. Does not contain -OL
    # tm_schedule: does not contain _1, _2. Does not contain -OL
    schedule = (
        gtfs.rename({"plan_trip_id": "trip_id", "stop_sequence": "gtfs_stop_sequence"})
        .with_columns(remove_rare_variant_route_suffix(pl.col("trip_id")))
        .join(
            tm.tm_schedule.collect(),
            on=["service_date", "trip_id", "stop_id", "plan_stop_departure_dt"],
            how="full",
            coalesce=True,
        )
        .with_columns(pl.coalesce("route_id", "route_id_right").alias("route_id"))
        # this operation fills in the nulls for the selected columns after the join- the commented out ones do not make sense to fill in
        # leaving them in as comments to make clear that this is a conscious choice
        .with_columns(
            pl.col(
                [
                    "trip_id",
                    "block_id",
                    "route_id",
                    "service_id",
                    "route_pattern_id",
                    "route_pattern_typicality",
                    "direction_id",
                    "direction",
                    "direction_destination",
                    "plan_stop_count",
                    "plan_start_time",
                    "plan_start_dt",
                    "pattern_id",
                    "trip_overload_id",
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
            pl.when(pl.col("gtfs_stop_sequence").is_null())
            .then(pl.lit("TM"))
            .when(pl.col("tm_stop_sequence").is_null())
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("schedule_joined"),
            pl.col("tm_stop_sequence")
            .fill_null(strategy="forward")
            .over(partition_by=["trip_id"], order_by=pl.col("plan_stop_departure_dt"))
            .alias("tm_filled_stop_sequence"),
        )
        .filter(
            pl.when(
                pl.col("trip_overload_id").max().over("trip_id").gt(0),  # for overloaded TM trips
            )
            .then(pl.col("schedule_joined").eq("JOIN"))  # take the records that matched GTFS Schedule
            .otherwise(pl.lit(True))  # keep all non-overloaded trips
        )
        .with_columns(
            pl.struct("tm_filled_stop_sequence", "plan_stop_departure_dt", "gtfs_stop_sequence", "plan_start_dt")
            .rank("min")
            .over(
                [
                    "trip_id",
                ]
            )
            .alias("stop_sequence"),
        )
        .with_columns(
            pl.when(pl.col("stop_sequence").eq(1))
            .then(pl.lit("start"))
            .when(pl.col("stop_sequence").eq(pl.col("stop_sequence").max().over(partition_by="trip_id")))
            .then(pl.lit("end"))
            .when(pl.coalesce("checkpoint_id", "timepoint_abbr").is_not_null())
            .then(pl.lit("mid"))
            .alias("point_type")
        )
    )

    valid = process_logger.log_dataframely_filter_results(*CombinedSchedule.filter(schedule))

    process_logger.log_complete()

    return valid
