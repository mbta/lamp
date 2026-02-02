import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_gtfs_rt import remove_rare_variant_route_suffix
from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterSchedule
from lamp_py.bus_performance_manager.events_gtfs_schedule import GTFSBusSchedule
from lamp_py.runtime_utils.process_logger import ProcessLogger


class CombinedBusSchedule(GTFSBusSchedule):
    """Union of GTFS and TransitMaster bus schedules."""

    gtfs_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    stop_sequence = dy.UInt32(primary_key=True, min=1)
    tm_pullout_id = dy.String(primary_key=True)
    vehicle_label = dy.String(nullable=True)
    schedule_joined = dy.String(nullable=False)
    tm_planned_sequence_start = dy.Int64(nullable=True)
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    tm_planned_sequence_end = dy.Int64(nullable=True)
    pattern_id = TransitMasterSchedule.pattern_id
    point_type = dy.String(nullable=True)
    timepoint_id = TransitMasterSchedule.timepoint_id
    timepoint_abbr = TransitMasterSchedule.timepoint_abbr
    timepoint_name = TransitMasterSchedule.timepoint_name
    tm_planned_sequence_end = TransitMasterSchedule.tm_planned_sequence_end
    tm_planned_sequence_start = TransitMasterSchedule.tm_planned_sequence_start
    timepoint_order = TransitMasterSchedule.timepoint_order
    plan_stop_departure_sam = dy.Int64(nullable=False)


# pylint: disable=R0801
def join_tm_schedule_to_gtfs_schedule(
    gtfs: dy.DataFrame[GTFSBusSchedule],
    tm_schedule: dy.DataFrame[TransitMasterSchedule],
    **debug_flags: dict[str, bool],
) -> dy.DataFrame[CombinedBusSchedule]:
    """
    Return a schedule including GTFS stops, TransitMaster timepoints, and shuttle trips not sourced from TransitMaster.

        :param gtfs: GTFSBusSchedule
        :param tm_schedule: TransitMasterSchedule

        :return CombinedBusSchedule:
    """
    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    process_logger = ProcessLogger("join_tm_schedule_to_gtfs_schedule")
    process_logger.log_start()

    if debug_flags.get("write_intermediates"):
        tm_schedule.write_parquet("/tmp/tm_schedule.parquet")

    tm_schedule_departure_dts = (
        tm_schedule.with_columns(plan_stop_departure_dt=pl.col("tm_stop_departure_dt"))
        .select("trip_id", "tm_stop_sequence", "plan_stop_departure_dt")
        .unique()
        .join(
            tm_schedule.select("trip_id", "stop_id", "tm_stop_sequence", "PULLOUT_ID"),
            on=["trip_id", "tm_stop_sequence"],
            how="inner",
        )
    )

    # gtfs_schedule: contains _1, _2. Does not contain -OL
    # tm_schedule: does not contain _1, _2. Does not contain -OL
    schedule = (
        pl.concat(
            [
                gtfs.with_columns(
                    gtfs_trip_suffix=pl.col("trip_id").str.extract(r"-OL(\d?)|_(\d)$"),
                    trip_id=remove_rare_variant_route_suffix(pl.col("trip_id")),
                )
                .join(tm_schedule, on="trip_id", how="semi")  # remove gtfs trips that arent in tm
                .join(
                    tm_schedule_departure_dts,
                    on=["trip_id", "stop_id", "plan_stop_departure_dt"],
                    how="inner",
                    coalesce=True,
                )
                .join(
                    tm_schedule.drop("route_id"),
                    on=["service_date", "trip_id", "PULLOUT_ID", "stop_id", "tm_stop_sequence"],
                    how="right",
                    coalesce=True,
                ),
                gtfs.join(tm_schedule, on="trip_id", how="anti"),  # add back in gtfs-only trips
            ],
            how="diagonal",
        )
        # this operation fills in the nulls for trip-level columns after the join
        # the stop-level values do not make sense to fill in
        # leaving the stop-level values as comments to make clear that this is a conscious choice:
        #
        # stop_id
        # stop_sequence
        # stop_name
        # plan_stop_departure_dt
        # plan_travel_time_seconds
        # plan_route_direction_headway_seconds
        # plan_direction_destination_headway_seconds
        .with_columns(
            pl.col(
                [
                    "service_date",
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
                ]
            )
            .fill_null(strategy="forward")  # handle added non-rev stops that are at the beginning
            .fill_null(strategy="backward")  # handle added non-rev stops that are at the end
            .over(["trip_id", "PULLOUT_ID"]),
            # add a column describing what data was used to form it.
            # to form the original datasets -
            # TM + JOIN = TM
            # GTFS + JOIN = GTFS
            pl.when(pl.col("gtfs_stop_sequence").is_null())
            .then(pl.lit("TM"))
            .when(pl.col("tm_stop_sequence").is_null())
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("schedule_joined"),
            pl.coalesce("tm_stop_departure_dt", "plan_stop_departure_dt").alias("plan_stop_departure_dt"),
        )
        .with_columns(
            pl.col("plan_stop_departure_dt")
            .sub(pl.col("service_date").cast(pl.Datetime))
            .dt.total_seconds()
            .alias("plan_stop_departure_sam"),
            pl.struct("plan_stop_departure_dt", "tm_stop_sequence", "gtfs_stop_sequence", "plan_start_dt")
            .rank("min")
            .over(["trip_id", "PULLOUT_ID"])
            .alias("stop_sequence"),
            pl.coalesce("PULLOUT_ID", pl.lit("____")).alias("tm_pullout_id"),
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

    valid = process_logger.log_dataframely_filter_results(*CombinedBusSchedule.filter(schedule, cast=True))

    process_logger.log_complete()

    return valid
