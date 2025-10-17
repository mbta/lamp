import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_tm import TransitMasterEvents
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import GTFSEvents
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.utils.filter_bank import SERVICE_DATE_END_HOUR


class BusEvents(CombinedSchedule, TransitMasterEvents):
    "Stop events from GTFS-RT, TransitMaster, and GTFS Schedule."
    trip_id = dy.String(primary_key=True)
    service_date = dy.Date(primary_key=True)
    stop_sequence = dy.UInt32(primary_key=True, min=1)
    vehicle_label = dy.String(primary_key=True)
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    gtfs_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    stop_count = dy.UInt32(nullable=True)
    start_time = dy.Int64(nullable=True)
    start_dt = dy.Datetime(nullable=True)
    direction_id = dy.Int8(nullable=True)
    direction = dy.String(nullable=True)
    vehicle_id = dy.String(nullable=True)
    gtfs_travel_to_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_arrival_dt = dy.Datetime(nullable=True, time_zone="UTC")
    gtfs_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    latitude = dy.Float64(nullable=True)
    longitude = dy.Float64(nullable=True)

    @dy.rule()
    def final_stop_has_arrival_dt() -> pl.Expr:  # pylint: disable=no-method-argument
        """
        The bus should have an arrival time to the final stop on the route if we have any GTFS-RT data for that stop.
        """
        return pl.when(
            pl.col("gtfs_stop_sequence").eq(pl.col("plan_stop_count")), pl.col("gtfs_travel_to_dt").is_not_null()
        ).then(pl.col("gtfs_arrival_dt").is_not_null())


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
        keys = ["trip_id", "tm_stop_sequence", "tm_actual_arrival_dt", "tm_actual_departure_dt", "tm_scheduled_time_dt"]

        missing_tm_events = self.tm.join(  # locate events that have mismatched event values
            self.bus, how="anti", on=keys, nulls_equal=True
        )

        return self.bus.join(missing_tm_events, how="anti", on=self.common_primary_keys())  # filter out those events


def join_rt_to_schedule(
    schedule: dy.DataFrame[CombinedSchedule], gtfs: dy.DataFrame[GTFSEvents], tm: dy.DataFrame[TransitMasterEvents]
) -> dy.DataFrame[BusEvents]:
    """
    Join gtfs-rt and transit master (tm) event dataframes using "asof" strategy for stop_sequence columns.
    There are frequent occasions where the gtfs_stop_sequence and tm_stop_sequence are not exactly the same.
    By matching the nearest stop sequence, we can align the two datasets.

    :return BusEvents:
    """

    # join gtfs and tm datasets using "asof" strategy for stop_sequence columns
    # asof strategy finds nearest value match between "asof" columns if exact match is not found
    # will perform regular left join on "by" columns

    # there are frequent occasions where the gtfs_stop_sequence and tm_stop_sequence are not exactly the same
    # usually off by 1 or so. By matching the nearest stop sequence
    # after grouping by trip, route, vehicle, and most importantly for sequencing - stop_id
    process_logger = ProcessLogger("join_rt_to_schedule")
    process_logger.log_start()

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

    # fill in vehicle_label and vehicle_id for each scheduled trip if available from gtfs events join -
    # this will allow TM join to join on vehicle id, which will eliminte multiple vehicle_id for the same trip_id
    # gtfs trip_ids may have ADDED or -OL trip_ids to denote added service,, but TM does not have those
    # trip_ids in its database, so overloads them into the existing trip_ids causing data inconsistency
    schedule_gtfs = schedule_gtfs.with_columns(
        pl.col(["vehicle_label", "vehicle_id"])
        .fill_null(strategy="forward")  # handle missing vehicle label at beginning
        .fill_null(strategy="backward")  # handle missing vehicle label at end
        .over(["trip_id"])
    )

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
            pl.col("tm_stop_sequence")
            .fill_null(strategy="forward")
            .over(partition_by=["service_date", "trip_id", "vehicle_label"], order_by=["gtfs_stop_sequence"])
            .alias("tm_filled_stop_sequence"),
            pl.coalesce(
                pl.col("service_date"),
                pl.when(pl.col("plan_start_dt").dt.hour() < SERVICE_DATE_END_HOUR)
                .then(pl.col("plan_start_dt").dt.offset_by("-1d").dt.date())
                .otherwise(pl.col("plan_start_dt").dt.date()),
            ).alias("service_date"),
            pl.coalesce(
                pl.col("gtfs_arrival_dt"),  # if gtfs_arrival_dt is null
                pl.when(
                    pl.col("gtfs_stop_sequence").eq(pl.col("plan_stop_count"))
                ).then(  # and it's the last stop on the route
                    pl.col("gtfs_in_transit_to_dts").struct.field("last_timestamp")
                ),  # use the last IN_TRANSIT_TO datetime
            ).alias("gtfs_arrival_dt"),
            pl.col("gtfs_in_transit_to_dts").struct.field("first_timestamp").alias("gtfs_travel_to_dt"),
        )
        .with_columns(
            pl.struct(pl.col("tm_filled_stop_sequence"), pl.col("gtfs_stop_sequence"))
            .rank("min")
            .over(["service_date", "trip_id", "vehicle_label"])
            .alias("stop_sequence"),
        )
        .select(BusEvents.column_names())
    )

    valid = process_logger.log_dataframely_filter_results(
        BusEvents.filter(schedule_gtfs_tm),
        BusPerformanceManager.filter({"tm": tm.lazy(), "bus": schedule_gtfs_tm.lazy(), "gtfs": gtfs.lazy()}),
    )

    process_logger.log_complete()

    return valid
