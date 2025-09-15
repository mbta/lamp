from typing import List

import dataframely as dy
import polars as pl

from lamp_py.bus_performance_manager.events_tm_schedule import TransitMasterTables
from lamp_py.runtime_utils.remote_files import (
    tm_trip_file,
    tm_vehicle_file,
    tm_work_piece_file,
    tm_block_file,
    tm_run_file,
    tm_operator_file,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger


class BusTrips(dy.Schema):
    "Common schema for bus schedule and event datasets."
    trip_id = dy.String(primary_key=True, nullable=False)
    stop_id = dy.String(nullable=False)
    route_id = dy.String(nullable=False)


class TransitMasterSchedule(BusTrips):
    "Scheduled stops in TransitMaster."
    timepoint_abbr = dy.String(nullable=True)
    timepoint_id = dy.Int64(nullable=True)
    timepoint_name = dy.String(nullable=True)
    timepoint_order = dy.UInt32(nullable=True)
    tm_stop_sequence = dy.Int64(primary_key=True, nullable=False)


class TransitMasterEvents(TransitMasterSchedule):
    "Scheduled and actual stops in TransitMaster."
    tm_actual_arrival_dt = dy.Datetime(nullable=True, time_zone="UTC")
    tm_actual_departure_dt = dy.Datetime(nullable=True, time_zone="UTC")
    tm_scheduled_time_dt = dy.Datetime(nullable=True, time_zone="UTC")
    tm_actual_arrival_time_sam = dy.Int64(nullable=True)
    tm_scheduled_time_sam = dy.Int64(nullable=True)
    tm_actual_departure_time_sam = dy.Int64(nullable=True)
    vehicle_label = dy.String(nullable=False)


def generate_tm_events(
    tm_files: List[str],
    tm_scheduled: TransitMasterTables,
) -> dy.DataFrame[TransitMasterEvents]:
    """
    Build out events from transit master stop crossing data after joining it
    with static Transit Master data describing stops, routes, trips, and
    vehicles.

    :param tm_files: transit master parquet files from the StopCrossings table.

    :return TransitMasterEvents:
    """
    logger = ProcessLogger("generate_tm_events", tm_files=tm_files)
    logger.log_start()

    # pull stop crossing information for a given service date and join it with
    # other dataframes using the transit master keys.
    #
    # convert the calendar id to a date object
    # remove leading zeros from route ids where they exist
    # convert arrival and departure times to utc datetimes
    # cast everything else as a string
    if len(tm_files) > 0:
        tm_stop_crossings = (
            pl.scan_parquet(tm_files)
            .filter(
                pl.col("ROUTE_ID").is_not_null()
                & pl.col("GEO_NODE_ID").is_not_null()
                & pl.col("TRIP_ID").is_not_null()
                & pl.col("VEHICLE_ID").is_not_null()
                & ((pl.col("ACT_ARRIVAL_TIME").is_not_null()) | (pl.col("ACT_DEPARTURE_TIME").is_not_null()))
            )
            .join(
                tm_scheduled.tm_routes,
                on="ROUTE_ID",
                how="left",
                coalesce=True,
            )
            .join(
                tm_scheduled.tm_vehicles,
                on="VEHICLE_ID",
                how="left",
                coalesce=True,
            )
            .join(
                tm_scheduled.tm_trip_geo_tp,
                on=["TRIP_ID", "TIME_POINT_ID", "GEO_NODE_ID", "PATTERN_GEO_NODE_SEQ"],
                how="left",
                coalesce=True,
            )
            .join(
                tm_scheduled.tm_sequences,
                on=["TRIP_ID"],
                how="left",
                coalesce=True,
            )
            .with_columns(
                (
                    pl.col("CALENDAR_ID")
                    .cast(pl.Utf8)
                    .str.slice(1)
                    .str.strptime(pl.Datetime, format="%Y%m%d")
                    .alias("service_date")
                ),
            )
            .collect()
        )
        tm_stop_crossings = tm_stop_crossings.select(
            (pl.col("ROUTE_ABBR").cast(pl.String).str.strip_chars_start("0").alias("route_id")),
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
            pl.col("GEO_NODE_ABBR").cast(pl.String).alias("stop_id"),
            pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
            pl.col("timepoint_order"),
            pl.col("tm_planned_sequence_start"),
            pl.col("tm_planned_sequence_end"),
            pl.col("PROPERTY_TAG").cast(pl.String).alias("vehicle_label"),
            pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
            pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
            pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
            (
                (pl.col("service_date") + pl.duration(seconds="SCHEDULED_TIME"))
                .dt.replace_time_zone("America/New_York", ambiguous="earliest")
                .dt.convert_time_zone("UTC")
                .alias("tm_scheduled_time_dt")
            ),
            (
                (pl.col("service_date") + pl.duration(seconds="ACT_ARRIVAL_TIME"))
                .dt.replace_time_zone("America/New_York", ambiguous="earliest")
                .dt.convert_time_zone("UTC")
                .alias("tm_actual_arrival_dt")
            ),
            (
                (pl.col("service_date") + pl.duration(seconds="ACT_DEPARTURE_TIME"))
                .dt.replace_time_zone("America/New_York", ambiguous="earliest")
                .dt.convert_time_zone("UTC")
                .alias("tm_actual_departure_dt")
            ),
            pl.col("SCHEDULED_TIME").cast(pl.Int64).alias("tm_scheduled_time_sam"),
            pl.col("ACT_ARRIVAL_TIME").cast(pl.Int64).alias("tm_actual_arrival_time_sam"),
            pl.col("ACT_DEPARTURE_TIME").cast(pl.Int64).alias("tm_actual_departure_time_sam"),
        )

    tm_stop_crossings = tm_stop_crossings.with_columns(
        pl.coalesce(
            pl.when(pl.col("tm_stop_sequence") == pl.col("tm_planned_sequence_start").min()).then(0),
            pl.when(pl.col("tm_stop_sequence") == pl.col("tm_planned_sequence_end").max()).then(2),
            pl.lit(1),
        )
        .over("trip_id", "vehicle_label")
        .alias("tm_point_type"),
    ).with_columns(
        pl.when((pl.col("tm_point_type") == 0).any() & (pl.col("tm_point_type") == 2).any())
        .then(1)
        .otherwise(0)
        .over("trip_id", "vehicle_label")
        .alias("is_full_trip")
    )

    valid, invalid = TransitMasterEvents.filter(tm_stop_crossings)

    logger.add_metadata(events_for_day=valid.height, invalidities=sum(invalid.counts().values()))
    logger.log_complete()
    return valid


def get_daily_work_pieces(daily_work_piece_files: List[str]) -> pl.DataFrame:
    """
    Create dataframe describing who drove what piece of work, run, and block.
    This dataframe can be joined against bus vehicle events by both trip id and
    vehicle label.

    :param daily_work_piece_files: transit master parquet files from the
        DailyWorkPiece table.

    :return dataframe:
        service_date -> Date
        tm_block_id -> String
        tm_run_id -> String
        tm_trip_id -> String
        operator_badge_number -> String
        tm_vehicle_label -> String
        logon_time -> Datetime(time_unit='us', time_zone=None) as UTC
        logoff_time -> Datetime(time_unit='us', time_zone=None) as UTC
    """
    # collect all the tables with static data on pieces of work, blocks, runs,
    # and trips. these will all be joined into a static work pieces dataframe
    # that will be joined against realtime data.

    # Work Piece Id is the TM Work Piece Table Key
    # Block Id is the TM Block Table Key
    # Run Id is the TM Run Table Key
    # Begin and End Time are in Seconds after Midnight. It will be used to
    #   filter a join with the Trips objects.
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    #
    # NOTE: RUN_IDs and BLOCK_IDs will be repeated, as multiple pieces of work
    # can have the same run or block. I think its because a Piece of Work can
    # be scheduled for a single day of the week but we reuse Runs and Blocks
    # across different scheduled days.
    tm_work_pieces = (
        pl.scan_parquet(tm_work_piece_file.s3_uri)
        .select(
            "WORK_PIECE_ID",
            "BLOCK_ID",
            "RUN_ID",
            "BEGIN_TIME",
            "END_TIME",
            "TIME_TABLE_VERSION_ID",
        )
        .unique()
    )

    # Block Id is the TM Block Table Key
    # Block Abbr is the ID the rest of the MBTA uses for this Block
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_blocks = (
        pl.scan_parquet(tm_block_file.s3_uri)
        .select(
            "BLOCK_ID",
            "BLOCK_ABBR",
            "TIME_TABLE_VERSION_ID",
        )
        .unique()
    )

    # Run Id is the TM Run Table Key
    # Run Designator is the ID the rest of the MBTA uses for this Run
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_runs = (
        pl.scan_parquet(tm_run_file.s3_uri)
        .select(
            "RUN_ID",
            "RUN_DESIGNATOR",
            "TIME_TABLE_VERSION_ID",
        )
        .unique()
    )

    # Trip Id is the TM Trip Table Key
    # Block Id is the TM Block Table Key
    # Trip Serial Number is the ID the rest of the MBTA uses for this Trip
    # Trip End Time is in Seconds after Midnight. It will be used to filter a
    #   join with the Work Pieces objects.
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_trips = (
        pl.scan_parquet(tm_trip_file.s3_uri)
        .select(
            "TRIP_ID",
            "BLOCK_ID",
            "TRIP_SERIAL_NUMBER",
            "TRIP_END_TIME",
            "TIME_TABLE_VERSION_ID",
        )
        .unique()
    )

    # Join all of the Static Data together to map a Trip to a Block, Run, and
    # Piece of Work.
    #
    # As multiple Pieces of Work will have the same Block and Run Ids, an
    # individual Trip, which is joined on Block Ids will map to multiple Pieces
    # of Work, we can filter out a lot of these based on the trip end time and
    # piece of work begin and end time. There may still be multiple pieces of
    # work per trip id though. I haven't found a good way to filter out
    static_work_pieces = (
        tm_work_pieces.join(
            tm_blocks,
            on=["BLOCK_ID", "TIME_TABLE_VERSION_ID"],
            coalesce=True,
        )
        .join(
            tm_runs,
            on=["RUN_ID", "TIME_TABLE_VERSION_ID"],
            coalesce=True,
        )
        .join(
            tm_trips,
            on=["BLOCK_ID", "TIME_TABLE_VERSION_ID"],
            coalesce=True,
        )
        .filter((pl.col("BEGIN_TIME") < pl.col("TRIP_END_TIME")) & (pl.col("END_TIME") >= pl.col("TRIP_END_TIME")))
    )

    # Collect the Realtime Details of who operated what vehicle for which piece
    # of work on a given day. Join the realtime data with static operator and
    # vehicle datasets.

    # Work Piece Id is the TM Work Piece Table Key
    # Calendar Id is the service date formatted "1YYYYMMDD"
    # Current Operator Id is a TM Operator Table Key
    # Run Id is the TM Run Table Key
    # Current Vehicle Id is a TM Vehicle Table Key
    # Actual Logon and Logoff Times are in Seconds after Midnight and describe
    #   when an operator logged on or off for this piece of work.
    #
    # NOTE: A Piece of Work can have multiple operator / vehicle pairs. The log
    # on and log off times can be used to figure out who is driving during a
    # vehicle event.
    daily_work_piece = (
        pl.scan_parquet(daily_work_piece_files)
        .filter(pl.col("WORK_PIECE_ID").is_not_null())
        .select(
            "WORK_PIECE_ID",
            "CALENDAR_ID",
            "CURRENT_OPERATOR_ID",
            "RUN_ID",
            "CURRENT_VEHICLE_ID",
            "ACTUAL_LOGON_TIME",
            "ACTUAL_LOGOFF_TIME",
        )
    )

    # Operator Id is the TM Operator Table Key
    # Operator Logon Id is the Badge Number
    tm_operators = (
        pl.scan_parquet(tm_operator_file.s3_uri)
        .select(
            "OPERATOR_ID",
            "ONBOARD_LOGON_ID",
        )
        .unique()
    )

    # Vehicle Id is the TM Vehicle Table Key
    # Property Tag is Vehicle Label used by the MBTA
    tm_vehicles = (
        pl.scan_parquet(tm_vehicle_file.s3_uri)
        .select(
            "VEHICLE_ID",
            "PROPERTY_TAG",
        )
        .unique()
    )

    # Join Operator and Vehicle information to the Daily Work Pieces
    realtime_work_pieces = daily_work_piece.join(
        tm_operators,
        left_on="CURRENT_OPERATOR_ID",
        right_on="OPERATOR_ID",
        how="left",
        coalesce=True,
    ).join(
        tm_vehicles,
        left_on="CURRENT_VEHICLE_ID",
        right_on="VEHICLE_ID",
        how="left",
        coalesce=True,
    )

    # Join the static and realtime workpiece dataframes on the Work Piece ID
    # and Run Id. This will give us a dataframe of potential operator / vehicle
    # pairs for a given trip, along with the block and run ids for a service
    # date. Since multiple operators can be associated with a single piece of
    # work, the logon and logoff times will need to be used to figure out who
    # was driving at a given time.
    return (
        realtime_work_pieces.join(
            static_work_pieces,
            on=["WORK_PIECE_ID", "RUN_ID"],
            how="left",
            coalesce=True,
        )
        .with_columns(
            (
                pl.col("CALENDAR_ID")
                .cast(pl.Utf8)
                .str.slice(1)
                .str.strptime(pl.Datetime, format="%Y%m%d")
                .alias("service_date")
            )
        )
        .select(
            pl.col("service_date").cast(pl.Date),
            pl.col("BLOCK_ABBR").cast(pl.String).alias("tm_block_id"),
            pl.col("RUN_DESIGNATOR").cast(pl.String).alias("tm_run_id"),
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("tm_trip_id"),
            (pl.col("ONBOARD_LOGON_ID").cast(pl.String).alias("operator_badge_number")),
            pl.col("PROPERTY_TAG").cast(pl.String).alias("tm_vehicle_label"),
            (
                (pl.col("service_date") + pl.duration(seconds="ACTUAL_LOGON_TIME"))
                .dt.replace_time_zone("America/New_York", ambiguous="earliest")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias("logon_time")
            ),
            (
                (pl.col("service_date") + pl.duration(seconds="ACTUAL_LOGOFF_TIME"))
                .dt.replace_time_zone("America/New_York", ambiguous="earliest")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
                .alias("logoff_time")
            ),
        )
        .collect()
    )
