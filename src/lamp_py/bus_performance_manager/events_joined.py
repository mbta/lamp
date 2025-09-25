import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_tm import TransitMasterEvents
from lamp_py.bus_performance_manager.combined_bus_schedule import CombinedSchedule
from lamp_py.bus_performance_manager.events_gtfs_rt import GTFSEvents
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.utils.filter_bank import SERVICE_DATE_END_HOUR


class BusEvents(CombinedSchedule, TransitMasterEvents, GTFSEvents):  # pylint: disable=too-many-ancestors
    "Stop events from GTFS-RT, TransitMaster, and GTFS Schedule."
    trip_id = dy.String(primary_key=True)
    service_date = dy.Date(primary_key=True)
    stop_sequences_vehicle_label_key = dy.String(
        primary_key=True, regex=r"[0-9_]{2}\|[0-9_]{2}\|(\w|_)+"
    )  # zero-padded tm_stop_sequence, zero-padded stop_sequence, and vehicle_label separated by |
    tm_stop_sequence = dy.Int64(nullable=True, primary_key=False)
    vehicle_label = dy.String(nullable=True, primary_key=False)
    stop_sequence = dy.Int64(nullable=True, primary_key=False)


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
    There are frequent occasions where the stop_sequence and tm_stop_sequence are not exactly the same.
    By matching the nearest stop sequence, we can align the two datasets.

    :return dataframe:
        service_date -> String
        route_id -> String
        trip_id -> String
        start_time -> Int64
        start_dt -> Datetime(time_unit='us', time_zone=None)
        stop_count -> UInt32
        direction_id -> Int8
        stop_id -> String
        stop_sequence -> Int64
        vehicle_id -> String
        vehicle_label -> String
        gtfs_travel_to_dt -> Datetime(time_unit='us', time_zone='UTC')
        gtfs_arrival_dt -> Datetime(time_unit='us', time_zone='UTC')
        latitude -> Float64
        longitude -> Float64
        tm_stop_sequence -> Int64
        timepoint_order -> UInt32
        tm_planned_sequence_start -> Int64
        tm_planned_sequence_end -> Int64
        timepoint_id -> Int64
        timepoint_abbr -> String
        timepoint_name -> String
        pattern_id -> Int64
        tm_scheduled_time_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_actual_arrival_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_actual_departure_dt -> Datetime(time_unit='us', time_zone='UTC')
        tm_scheduled_time_sam -> Int64
        tm_actual_arrival_time_sam -> Int64
        tm_actual_departure_time_sam -> Int64
        tm_point_type -> Int32
        is_full_trip -> Int32
        schedule_joined -> Boolean
    """

    # join gtfs and tm datasets using "asof" strategy for stop_sequence columns
    # asof strategy finds nearest value match between "asof" columns if exact match is not found
    # will perform regular left join on "by" columns

    # there are frequent occasions where the stop_sequence and tm_stop_sequence are not exactly the same
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
        schedule_vehicles.sort(by="stop_sequence")
        .join_asof(
            gtfs.sort(by="stop_sequence"),
            on="stop_sequence",
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
        .drop(
            "route_id_right_tm",
            "timepoint_order_right_tm",
            "timepoint_id_right_tm",
            "timepoint_abbr_right_tm",
            "timepoint_name_right_tm",
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.coalesce(pl.col("tm_stop_sequence").cast(pl.String).str.zfill(2), pl.lit("__")),
                    pl.coalesce(pl.col("stop_sequence").cast(pl.String).str.zfill(2), pl.lit("__")),
                    pl.coalesce(pl.col("vehicle_label"), pl.lit("_____")),
                ],
                separator="|",
            ).alias("stop_sequences_vehicle_label_key"),
            pl.coalesce(
                pl.col("service_date"),
                pl.when(pl.col("plan_start_dt").dt.hour() < SERVICE_DATE_END_HOUR)
                .then(pl.col("plan_start_dt").dt.offset_by("-1d").dt.date())
                .otherwise(pl.col("plan_start_dt").dt.date()),
            ).alias("service_date"),
        )
    )

    valid, invalid = BusEvents.filter(schedule_gtfs_tm)

    process_logger.add_metadata(valid_records=valid.height, validation_errors=sum(invalid.counts().values()))

    if invalid.counts():
        process_logger.log_failure(dy.exc.ValidationError(", ".join(invalid.counts().keys())))

    valid_collection = BusPerformanceManager.is_valid(
        {"tm": tm.lazy(), "bus": schedule_gtfs_tm.lazy(), "gtfs": gtfs.lazy()}
    )

    if not valid_collection:
        process_logger.log_failure(dy.exc.ValidationError(BusPerformanceManager.__name__ + " failed validation"))

    process_logger.log_complete()

    return valid
