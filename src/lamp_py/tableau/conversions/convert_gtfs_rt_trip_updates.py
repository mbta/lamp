import dataframely as dy
import polars as pl

from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter
from lamp_py.runtime_utils.process_logger import ProcessLogger


class RailTripUpdateBase(dy.Schema):
    "Intersection of descendant rail schemas."
    id = dy.String(nullable=True)
    trip_id = dy.String(nullable=True, alias="trip_update.trip.trip_id")
    route_id = dy.String(nullable=True, alias="trip_update.trip.route_id")
    direction_id = dy.UInt8(nullable=True, alias="trip_update.trip.direction_id")
    start_time = dy.String(nullable=True, alias="trip_update.trip.start_time")
    start_date = dy.String(nullable=True, alias="trip_update.trip.start_date")
    schedule_relationship = dy.String(nullable=True, alias="trip_update.trip.schedule_relationship")
    revenue = dy.Bool(nullable=True, alias="trip_update.trip.revenue")
    vehicle_id = dy.String(nullable=True, alias="trip_update.vehicle.id")
    vehicle_label = dy.String(nullable=True, alias="trip_update.vehicle.label")
    timestamp = dy.Datetime(nullable=True, alias="trip_update.timestamp")
    feed_timestamp = dy.Datetime(nullable=True)
    stop_id = dy.String(nullable=True, alias="trip_update.stop_time_update.stop_id")
    departure_time = dy.Datetime(nullable=True, alias="trip_update.stop_time_update.departure.time")
    departure_uncertainty = dy.Int32(nullable=True, alias="trip_update.stop_time_update.departure.uncertainty")
    stop_schedule_relationship = dy.String(nullable=True, alias="trip_update.stop_time_update.schedule_relationship")
    boarding_status = dy.String(nullable=True, alias="trip_update.stop_time_update.boarding_status")
    feed_timestamp_first_prediction = dy.Datetime(nullable=True)
    feed_timestamp_last_prediction = dy.Datetime(nullable=True)


class LightRailTerminalTripUpdates(RailTripUpdateBase):
    "Analytical dataset for LRTP dashboards."
    stop_id = dy.String(
        nullable=True,
        alias="trip_update.stop_time_update.stop_id",
        check=lambda x: x.is_in(LightRailFilter.terminal_stop_ids),
    )


class HeavyRailTerminalTripUpdates(RailTripUpdateBase):
    "Analytical dataset for heavy rail and light rail midpoint dashboards."
    departure_time = dy.Datetime(
        nullable=True,
        alias="trip_update.stop_time_update.departure.time",
        check=lambda x: x.is_not_null(),  # setting field nullability directly prevents writing with pyarrow; remove explicit pyarrow schema validation once all datasets use dataframely validation
    )
    stop_id = dy.String(
        nullable=True,
        alias="trip_update.stop_time_update.stop_id",
        check=lambda x: x.is_in(HeavyRailFilter.terminal_stop_ids),
    )
    route_pattern_id = dy.String(nullable=True, alias="trip_update.trip.route_pattern_id")
    tm_trip_id = dy.String(nullable=True, alias="trip_update.trip.tm_trip_id")
    overload_id = dy.Int64(nullable=True, alias="trip_update.trip.overload_id")
    overload_offset = dy.Int64(nullable=True, alias="trip_update.trip.overload_offset")
    last_trip = dy.Bool(nullable=True, alias="trip_update.trip.last_trip")
    license_plate = dy.String(nullable=True, alias="trip_update.vehicle.license_plate")
    assignment_status = dy.String(nullable=True, alias="trip_update.vehicle.assignment_status")
    delay = dy.Int32(nullable=True, alias="trip_update.delay")
    stop_sequence = dy.UInt32(nullable=True, alias="trip_update.stop_time_update.stop_sequence")
    arrival_delay = dy.Int32(nullable=True, alias="trip_update.stop_time_update.arrival.delay")
    arrival_time = dy.Datetime(nullable=True, alias="trip_update.stop_time_update.arrival.time")
    arrival_uncertainty = dy.Int32(nullable=True, alias="trip_update.stop_time_update.arrival.uncertainty")
    departure_delay = dy.Int32(nullable=True, alias="trip_update.stop_time_update.departure.delay")


def lrtp_prod(polars_df: pl.DataFrame) -> dy.DataFrame[LightRailTerminalTripUpdates]:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    process_logger = ProcessLogger("lrtp_prod")
    process_logger.log_start()

    polars_df = polars_df.filter(
        pl.col("trip_update.stop_time_update.stop_id").is_in(LightRailFilter.terminal_stop_ids)
    )
    polars_df = append_prediction_valid_duration(polars_df)
    polars_df = apply_timezone_conversions(polars_df)
    valid = LightRailTerminalTripUpdates.validate(polars_df)

    process_logger.log_complete()

    return valid


def lrtp_devgreen(trip_updates: pl.DataFrame) -> dy.DataFrame[LightRailTerminalTripUpdates]:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    This is intended for more complicated transformations than is feasible to perform in pyarrow

    Parameters
    ----------
    polars_df : Dataframe filtered down to light rail trip updates

    Returns
    -------
    pl.Dataframe : filtered down to departures at terminals
                   add feed_timestamp.first_prediction and feed_timestamp.last_prediction columns
                   to signify a validity duration of a prediction

    """
    process_logger = ProcessLogger("lrtp_devgreen")
    process_logger.log_start()

    def temporary_lrtp_assign_new_trip_ids(trip_updates: pl.DataFrame, threshold_sec: int = 60 * 15) -> pl.DataFrame:
        trip_updates = (
            trip_updates.sort("trip_update.timestamp")
            .with_columns(
                pl.col("trip_update.timestamp")
                .diff()
                .alias("mdiff")
                .over("trip_update.trip.trip_id", "trip_update.trip.start_date")
            )
            .with_columns(
                pl.when(pl.col("mdiff") < threshold_sec)
                .then(0)
                .otherwise(1)
                .fill_null(0)
                .cum_sum()
                .cast(pl.String)
                .alias("new_id")
                .over("trip_update.trip.trip_id", "trip_update.trip.start_date")
            )
            .with_columns(
                pl.when(pl.col("new_id").ne("1"))
                .then(
                    pl.concat_str(
                        [pl.col("trip_update.trip.trip_id"), pl.lit("_LAMP"), pl.col("new_id")], ignore_nulls=True
                    )
                )
                .alias("trip_update.trip.trip_id1")
            )
            .with_columns(
                pl.coalesce("trip_update.trip.trip_id1", "trip_update.trip.trip_id").alias("trip_update.trip.trip_id")
            )
            .drop("trip_update.trip.trip_id1")
        )
        return trip_updates

    trip_updates = apply_timezone_conversions(trip_updates)
    # filter down to only terminals - original data
    trip_updates = trip_updates.filter(
        pl.col("trip_update.stop_time_update.departure.time").is_not_null(),
        pl.col("trip_update.stop_time_update.stop_id").is_in(LightRailFilter.terminal_stop_ids),
        pl.col("trip_update.trip.revenue"),
        pl.col("trip_update.trip.schedule_relationship").ne("CANCELED"),
        pl.col("trip_update.stop_time_update.schedule_relationship").ne("SKIPPED"),
        pl.col("trip_update.stop_time_update.departure.time").sub(pl.col("feed_timestamp")).dt.total_seconds().ge(1),
    )
    trip_updates = temporary_lrtp_assign_new_trip_ids(trip_updates)
    trip_updates = append_prediction_valid_duration(trip_updates)
    valid = LightRailTerminalTripUpdates.validate(trip_updates)

    process_logger.log_complete()

    return valid


def heavyrail(polars_df: pl.DataFrame) -> dy.DataFrame[HeavyRailTerminalTripUpdates]:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    process_logger = ProcessLogger("heavyrail")
    process_logger.log_start()

    polars_df = polars_df.filter(
        ~pl.col("trip_update.stop_time_update.departure.time").is_null()
        & pl.col("trip_update.stop_time_update.stop_id").is_in(HeavyRailFilter.terminal_stop_ids)
    )
    polars_df = apply_timezone_conversions(polars_df)
    valid = HeavyRailTerminalTripUpdates.validate(polars_df)

    process_logger.log_complete()

    return valid


def apply_timezone_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply timezone conversions to lamp data before outputting for tableau consumption
    """
    polars_df = polars_df.with_columns(
        pl.from_epoch(pl.col("trip_update.stop_time_update.departure.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.stop_time_update.arrival.time"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("trip_update.timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp_first_prediction"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
        pl.from_epoch(pl.col("feed_timestamp_last_prediction"), time_unit="s")
        .dt.convert_time_zone(time_zone="US/Eastern")
        .dt.replace_time_zone(None),
    )
    return polars_df


def append_prediction_valid_duration(trip_updates: pl.DataFrame) -> pl.DataFrame:
    """
    Append feed_timestamp_first_prediction and feed_timestamp_last_prediction columns to the dataframe

    Predictions are valid only instantaneously from the upstream producer (RTR)
    This method attempts to derive a rough "validity period" by checking when the
    first prediction is made vs the last one grouped by trip_id, feed_timestamp, and predicted time

    The intent is to isolate timestamps that are sent by RTR that are meant for the
    same trip_id, but has not changed the prediction (departure.time) since the previous query.

    Parameters
    ----------
    polars_df : Dataframe filtered down to light rail trip updates

    Returns
    -------
    pl.Dataframe : dataframe with feed_timestamp_first_prediction and feed_timestamp_last_prediction columns
                to signify a validity duration of a prediction
                these columns are all null if enable_calculation argument is False
                these columns are calculated and valid if enable_calculation is True
    """

    trip_updates = trip_updates.with_columns(
        pl.col("feed_timestamp")
        .min()
        .over(["trip_update.trip.trip_id", "trip_update.timestamp", "trip_update.stop_time_update.departure.time"])
        .alias("feed_timestamp_first_prediction"),
        pl.col("feed_timestamp")
        .max()
        .over(["trip_update.trip.trip_id", "trip_update.timestamp", "trip_update.stop_time_update.departure.time"])
        .alias("feed_timestamp_last_prediction"),
    ).sort(["trip_update.trip.trip_id", "trip_update.stop_time_update.stop_sequence", "feed_timestamp"])

    return trip_updates
