import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_tm import (
    TMDailyWorkPiece,
    TransitMasterEvents,
)
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedBusSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import GTFSEvents, remove_overload_and_rare_variant_suffix
from lamp_py.runtime_utils.process_logger import ProcessLogger


class BusEvents(CombinedBusSchedule, TransitMasterEvents):
    "Stop events from GTFS-RT, TransitMaster, and GTFS Schedule."
    trip_id = dy.String(primary_key=True)
    vehicle_label = dy.String(primary_key=True)
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    gtfs_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    stop_count = dy.UInt32(nullable=True)
    start_time = dy.Int64(nullable=True)
    start_dt = dy.Datetime(nullable=True)
    direction_id = dy.Int8(nullable=True)
    direction = dy.String(nullable=True)
    vehicle_id = dy.String(nullable=True)
    gtfs_first_in_transit_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_last_in_transit_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_arrival_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    latitude = dy.Float64(nullable=True)
    longitude = dy.Float64(nullable=True)
    trip_id_gtfs = dy.String(nullable=True)
    public_operator_id = dy.Int64(nullable=True)

    # pylint: disable=no-method-argument

    @dy.rule()
    def final_stop_has_arrival_dt() -> pl.Expr:
        """
        The bus should have an arrival time to the final stop on the route if we have any GTFS-RT data for that stop.
        """
        return pl.when(
            pl.col("point_type").eq(pl.lit("end")),
            pl.col("gtfs_last_in_transit_dt").is_not_null(),
        ).then(pl.col("gtfs_arrival_dt").is_not_null())

    @dy.rule()
    def _no_ol_trip_ids() -> pl.Expr:
        return ~pl.col("trip_id").str.contains("OL")

    @dy.rule()
    def _no_split_trips1() -> pl.Expr:
        return ~pl.col("trip_id").str.ends_with("_1")

    @dy.rule()
    def _no_split_trips2() -> pl.Expr:
        return ~pl.col("trip_id").str.ends_with("_2")

    # pylint: enable=no-method-argument


class BusPerformanceManager(dy.Collection):
    "Relationships between BusPM datasets."
    tm: dy.LazyFrame[TransitMasterEvents]
    bus: dy.LazyFrame[BusEvents]
    gtfs: dy.LazyFrame[GTFSEvents]

    @dy.filter()
    def preserve_all_trips(self) -> pl.LazyFrame:
        "If trips appear in GTFS or TM, then they appear in downstream records."
        missing_gtfs_trips = (
            self.gtfs.select(self.common_primary_keys())
            .unique()
            .join(self.bus, how="anti", on=self.common_primary_keys())
        )
        missing_tm_trips = (
            self.tm.select(self.common_primary_keys())
            .unique()
            .join(self.bus, how="anti", on=self.common_primary_keys())
        )

        return (
            self.bus.select(self.common_primary_keys())
            .unique()
            .join(pl.concat([missing_gtfs_trips, missing_tm_trips]), on=self.common_primary_keys(), how="anti")
        )

    @dy.filter()
    def preserve_tm_events(self) -> pl.LazyFrame:
        "If values in TransitMaster are not null, then downstream records should also not be null for those columns."
        keys = [
            "trip_id",
            "tm_stop_sequence",
            "tm_actual_arrival_dt",
            "tm_actual_departure_dt",
        ]

        missing_tm_events = self.tm.join(  # locate events that have mismatched event values
            self.bus, how="anti", on=keys, nulls_equal=True
        )

        return self.bus.join(missing_tm_events, how="anti", on=self.common_primary_keys())  # filter out those events


def join_rt_to_schedule(
    schedule: dy.DataFrame[CombinedBusSchedule],
    gtfs: dy.DataFrame[GTFSEvents],
    tm: dy.DataFrame[TransitMasterEvents],
    tm_operator: dy.DataFrame[TMDailyWorkPiece],
) -> dy.DataFrame[BusEvents]:
    """
    Join gtfs-rt and transit master (tm) event dataframes using "asof" strategy for stop_sequence columns.
    There are frequent occasions where the gtfs_stop_sequence and tm_stop_sequence are not exactly the same.
    By matching the nearest stop sequence, we can align the two datasets.

    :return BusEvents:
    """

    # there are frequent occasions where the gtfs_stop_sequence and tm_stop_sequence are not exactly the same
    # usually off by 1 or so. By matching the nearest stop sequence
    # after grouping by trip, route, vehicle, and most importantly for sequencing - stop_id
    process_logger = ProcessLogger("join_rt_to_schedule")
    process_logger.log_start()

    # replace both now
    gtfs = gtfs.with_columns(  # type: ignore[assignment]
        pl.col("trip_id").alias("trip_id_gtfs"),
        remove_overload_and_rare_variant_suffix(pl.col("trip_id")),
    )
    schedule_vehicles = schedule.join(
        pl.concat(
            [tm.select("trip_id", "vehicle_label", "stop_id"), gtfs.select("trip_id", "vehicle_label", "stop_id")]
        ).unique(),
        how="left",
        on=["trip_id", "stop_id"],
        coalesce=True,
    )

    schedule_gtfs = (
        schedule_vehicles.sort(by="gtfs_stop_sequence")
        .join_asof(
            gtfs.sort(by="gtfs_stop_sequence"),
            on="gtfs_stop_sequence",
            by=["trip_id", "stop_id", "vehicle_label"],
            strategy="nearest",
            coalesce=True,
            suffix="_right_gtfs",
        )
        .drop(
            "route_id_right_gtfs",
            "direction_id_right_gtfs",
        )
    )

    # join gtfs and tm datasets using "asof" strategy for stop_sequence columns
    # asof strategy finds nearest value match between "asof" columns if exact match is not found
    # will perform regular left join on "by" columns
    schedule_gtfs_tm = (
        schedule_gtfs.sort(by="tm_stop_sequence")
        .join_asof(
            tm.sort(by="tm_stop_sequence"),
            on="tm_stop_sequence",
            by=["trip_id", "stop_id", "vehicle_label"],
            strategy="nearest",
            coalesce=True,
            suffix="_right_tm",
        )
        .with_columns(
            pl.coalesce("vehicle_label", pl.lit("____")).alias("vehicle_label"),
            pl.col("stop_sequence").max().over(partition_by=["trip_id", "vehicle_label"]).alias("stop_count"),
            pl.coalesce(
                pl.col("gtfs_arrival_dt"),  # if gtfs_arrival_dt is null
                pl.when(pl.col("point_type").eq(pl.lit("end"))).then(  # and it's the last stop on the route
                    pl.col("gtfs_last_in_transit_dt")
                ),  # use the last IN_TRANSIT_TO datetime
            ).alias("gtfs_arrival_dt"),
        )
        # brings in public_operator_id
        .join(
            tm_operator.select("tm_trip_id", "tm_vehicle_label", "public_operator_id").unique(),
            left_on=["trip_id", "vehicle_label"],
            right_on=["tm_trip_id", "tm_vehicle_label"],
            how="left",
        )
        .select(BusEvents.column_names())
    )

    valid = process_logger.log_dataframely_filter_results(*BusEvents.filter(schedule_gtfs_tm))

    process_logger.log_complete()

    return valid
