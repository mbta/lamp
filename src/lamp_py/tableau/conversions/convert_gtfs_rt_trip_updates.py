import pyarrow
import polars as pl

from lamp_py.utils.filter_bank import HeavyRailFilter, LightRailFilter

gtfs_rt_trip_updates_processed_schema = pyarrow.schema(
    [
        ("id", pyarrow.large_string()),
        ("trip_update.trip.trip_id", pyarrow.large_string()),
        ("trip_update.trip.route_id", pyarrow.large_string()),
        ("trip_update.trip.direction_id", pyarrow.uint8()),
        ("trip_update.trip.start_time", pyarrow.large_string()),
        ("trip_update.trip.start_date", pyarrow.large_string()),
        ("trip_update.trip.schedule_relationship", pyarrow.large_string()),
        ("trip_update.trip.route_pattern_id", pyarrow.large_string()),
        ("trip_update.trip.tm_trip_id", pyarrow.large_string()),
        ("trip_update.trip.overload_id", pyarrow.int64()),
        ("trip_update.trip.overload_offset", pyarrow.int64()),
        ("trip_update.trip.revenue", pyarrow.bool_()),
        ("trip_update.trip.last_trip", pyarrow.bool_()),
        ("trip_update.vehicle.id", pyarrow.large_string()),
        ("trip_update.vehicle.label", pyarrow.large_string()),
        ("trip_update.vehicle.license_plate", pyarrow.large_string()),
        # trip_update.vehicle.consist: list<element: struct<label: string>>
        #   child 0, element: struct<label: string>
        #       child 0, label: string
        ("trip_update.vehicle.assignment_status", pyarrow.large_string()),
        ("trip_update.timestamp", pyarrow.timestamp("us")),
        ("trip_update.delay", pyarrow.int32()),
        ("feed_timestamp", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.stop_sequence", pyarrow.uint32()),
        ("trip_update.stop_time_update.stop_id", pyarrow.large_string()),
        ("trip_update.stop_time_update.arrival.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.arrival.time", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.arrival.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.time", pyarrow.timestamp("us")),
        ("trip_update.stop_time_update.departure.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.schedule_relationship", pyarrow.large_string()),
        ("trip_update.stop_time_update.boarding_status", pyarrow.large_string()),
        ("feed_timestamp_first_prediction", pyarrow.timestamp("us")),
        ("feed_timestamp_last_prediction", pyarrow.timestamp("us")),
    ]
)


def lrtp_prod(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    polars_df = polars_df.filter(
        pl.col("trip_update.stop_time_update.stop_id").is_in(LightRailFilter.terminal_stop_ids)
    )
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


def lrtp_devgreen(polars_df: pl.DataFrame) -> pl.DataFrame:
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

    # filter down to only terminals - original data
    polars_df = polars_df.filter(
        ~pl.col("trip_update.stop_time_update.departure.time").is_null()
        & pl.col("trip_update.stop_time_update.stop_id").is_in(LightRailFilter.terminal_stop_ids)
    )
    polars_df = append_prediction_valid_duration(polars_df)
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


def heavyrail(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """

    polars_df = polars_df.filter(
        ~pl.col("trip_update.stop_time_update.departure.time").is_null()
        & pl.col("trip_update.stop_time_update.stop_id").is_in(HeavyRailFilter.terminal_stop_ids)
    )
    polars_df = apply_timezone_conversions(polars_df)
    return polars_df


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


def schema() -> pyarrow.schema:
    """
    Function returns schema of the processed gtfs-rt trip updates
    """
    return gtfs_rt_trip_updates_processed_schema


def append_prediction_valid_duration(polars_df: pl.DataFrame) -> pl.DataFrame:
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

    prediction_valid_duration = polars_df.group_by(
        ["trip_update.trip.trip_id", "trip_update.timestamp", "trip_update.stop_time_update.departure.time"]
    ).agg(
        pl.col("feed_timestamp").min().alias("feed_timestamp_first_prediction"),
        pl.col("feed_timestamp").max().alias("feed_timestamp_last_prediction"),
    )

    polars_df = polars_df.join(
        prediction_valid_duration,
        on=["trip_update.trip.trip_id", "trip_update.timestamp", "trip_update.stop_time_update.departure.time"],
        how="inner",
        coalesce=True,
    ).sort(["trip_update.trip.trip_id", "trip_update.stop_time_update.stop_sequence", "feed_timestamp"])

    return polars_df
