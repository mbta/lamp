from typing import List

import pytz
import polars as pl

from lamp_py.runtime_utils.remote_files import RemoteFileLocations

BOSTON_TZ = pytz.timezone("EST5EDT")
UTC_TZ = pytz.utc


def create_dt_from_sam(
    service_date_col: pl.Expr, sam_time_col: pl.Expr
) -> pl.Expr:
    """
    add a seconds after midnight to a service date to create a datetime object.
    seconds after midnight is in boston local time, convert it to utc.
    """
    return (
        service_date_col.cast(pl.Datetime) + pl.duration(seconds=sam_time_col)
    ).map_elements(
        lambda x: BOSTON_TZ.localize(x).astimezone(UTC_TZ),
        return_dtype=pl.Datetime,
    )


def generate_tm_events(tm_files: List[str]) -> pl.DataFrame:
    """
    Build out events from transit master stop crossing data after joining it
    with static Transit Master data describing stops, routes, trips, and
    vehicles.

    :param tm_files: transit master parquet files from the StopCrossings table.

    :return dataframe:
        service_date -> Date
        begin_wp_sam -> Int64
        end_wp_skm -> Int64
        block_id -> String
        run_id -> String
        trip_id -> String
        operator_badge_number -> String
        vehicle_label -> String
        logon_sam -> Int64
        logoff_sam -> Int64
        logon_time -> Datetime(time_unit='us', time_zone=None)
        logoff_time -> Datetime(time_unit='us', time_zone=None))
    """
    # the geo node id is the transit master key and the geo node abbr is the
    # gtfs stop id
    tm_geo_nodes = pl.scan_parquet(
        RemoteFileLocations.tm_geo_node_file.get_s3_path()
    ).select(["GEO_NODE_ID", "GEO_NODE_ABBR"])

    # the route id is the transit master key and the route abbr is the gtfs
    # route id.
    # NOTE: some of these route ids have leading zeros
    tm_routes = pl.scan_parquet(
        RemoteFileLocations.tm_route_file.get_s3_path()
    ).select(["ROUTE_ID", "ROUTE_ABBR"])

    # the trip id is the transit master key and the trip serial number is the
    # gtfs trip id.
    tm_trips = pl.scan_parquet(
        RemoteFileLocations.tm_trip_file.get_s3_path()
    ).select(["TRIP_ID", "TRIP_SERIAL_NUMBER"])

    # the vehicle id is the transit master key and the property tag is the
    # vehicle label
    tm_vehicles = pl.scan_parquet(
        RemoteFileLocations.tm_vehicle_file.get_s3_path()
    ).select(["VEHICLE_ID", "PROPERTY_TAG"])

    # pull stop crossing information for a given service date and join it with
    # other dataframes using the transit master keys.
    #
    # convert the calendar id to a date object
    # remove leading zeros from route ids where they exist
    # convert arrival and departure times to utc datetimes
    # cast everything else as a string
    tm_stop_crossings = (
        pl.scan_parquet(tm_files)
        .filter(
            pl.col("ACT_ARRIVAL_TIME").is_not_null()
            | pl.col("ACT_DEPARTURE_TIME").is_not_null()
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID")
        .join(tm_routes, on="ROUTE_ID")
        .join(tm_trips, on="TRIP_ID", how="left", coalesce=True)
        .join(tm_vehicles, on="VEHICLE_ID")
        .select(
            pl.col("CALENDAR_ID")
            .cast(pl.Utf8)
            .str.slice(1)
            .str.strptime(pl.Date, format="%Y%m%d")
            .alias("service_date"),
            pl.col("ACT_ARRIVAL_TIME").alias("arrival_sam"),
            pl.col("ACT_DEPARTURE_TIME").alias("departure_sam"),
            pl.col("PROPERTY_TAG").cast(pl.String).alias("vehicle_label"),
            pl.col("ROUTE_ABBR")
            .cast(pl.String)
            .str.strip_chars_start("0")
            .alias("route_id"),
            pl.col("GEO_NODE_ID").cast(pl.String).alias("geo_node_id"),
            pl.col("GEO_NODE_ABBR").cast(pl.String).alias("stop_id"),
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
        )
        .with_columns(
            create_dt_from_sam(
                pl.col("service_date"), pl.col("arrival_sam")
            ).alias("arrival_tm"),
            create_dt_from_sam(
                pl.col("service_date"), pl.col("departure_sam")
            ).alias("departure_tm"),
        )
        .collect()
    )

    return tm_stop_crossings


def get_daily_work_pieces(daily_work_piece_files: List[str]) -> pl.DataFrame:
    """
    Create dataframe describing who drove what piece of work, run, and block.
    This dataframe can be joined against bus vehicle events by both trip id and
    vehicle label.

    :param daily_work_piece_files: transit master parquet files from the
        DailyWorkPiece table.

    :return dataframe:
        service_date -> Date
        block_id -> String
        run_id -> String
        trip_id -> String
        operator_badge_number -> String
        vehicle_label -> String
        logon_sam -> Int64
        logoff_sam -> Int64
        logon_time -> Datetime(time_unit='us', time_zone=None)
        logoff_time -> Datetime(time_unit='us', time_zone=None
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
    tm_work_pieces = pl.scan_parquet(
        RemoteFileLocations.tm_work_piece_file.get_s3_path()
    ).select(
        "WORK_PIECE_ID",
        "BLOCK_ID",
        "RUN_ID",
        "BEGIN_TIME",
        "END_TIME",
        "TIME_TABLE_VERSION_ID",
    )

    # Block Id is the TM Block Table Key
    # Block Abbr is the ID the rest of the MBTA uses for this Block
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_blocks = pl.scan_parquet(
        RemoteFileLocations.tm_block_file.get_s3_path()
    ).select("BLOCK_ID", "BLOCK_ABBR", "TIME_TABLE_VERSION_ID")

    # Run Id is the TM Run Table Key
    # Run Designator is the ID the rest of the MBTA uses for this Run
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_runs = pl.scan_parquet(
        RemoteFileLocations.tm_run_file.get_s3_path()
    ).select("RUN_ID", "RUN_DESIGNATOR", "TIME_TABLE_VERSION_ID")

    # Trip Id is the TM Trip Table Key
    # Block Id is the TM Block Table Key
    # Trip Serial Number is the ID the rest of the MBTA uses for this Trip
    # Trip End Time is in Seconds after Midnight. It will be used to filter a
    #   join with the Work Pieces objects.
    # Time Table Version Id is similar to our Static Schedule Version keys in
    #   the Rail Performance Manager DB
    tm_trips = pl.scan_parquet(
        RemoteFileLocations.tm_trip_file.get_s3_path()
    ).select(
        "TRIP_ID",
        "BLOCK_ID",
        "TRIP_SERIAL_NUMBER",
        "TRIP_END_TIME",
        "TIME_TABLE_VERSION_ID",
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
        tm_work_pieces.join(tm_blocks, on=["BLOCK_ID", "TIME_TABLE_VERSION_ID"])
        .join(tm_runs, on=["RUN_ID", "TIME_TABLE_VERSION_ID"])
        .join(tm_trips, on=["BLOCK_ID", "TIME_TABLE_VERSION_ID"])
        .filter(
            (pl.col("BEGIN_TIME") < pl.col("TRIP_END_TIME"))
            & (pl.col("END_TIME") >= pl.col("TRIP_END_TIME"))
        )
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
    tm_operators = pl.scan_parquet(
        RemoteFileLocations.tm_operator_file.get_s3_path()
    ).select("OPERATOR_ID", "ONBOARD_LOGON_ID")

    # Vehicle Id is the TM Vehicle Table Key
    # Property Tag is Vehicle Label used by the MBTA
    tm_vehicles = pl.scan_parquet(
        RemoteFileLocations.tm_vehicle_file.get_s3_path()
    ).select("VEHICLE_ID", "PROPERTY_TAG")

    # Join Operator and Vehicle information to the Daily Work Pieces
    realtime_work_pieces = daily_work_piece.join(
        tm_operators, left_on="CURRENT_OPERATOR_ID", right_on="OPERATOR_ID"
    ).join(tm_vehicles, left_on="CURRENT_VEHICLE_ID", right_on="VEHICLE_ID")

    # Join the static and realtime workpiece dataframes on the Work Piece ID
    # and Run Id. This will give us a dataframe of potential operator / vehicle
    # pairs for a given trip, along with the block and run ids for a service
    # date. Since multiple operators can be associated with a single piece of
    # work, the logon and logoff times will need to be used to figure out who
    # was driving at a given time.
    return (
        realtime_work_pieces.join(
            static_work_pieces, on=["WORK_PIECE_ID", "RUN_ID"], how="left"
        )
        .select(
            pl.col("CALENDAR_ID")
            .cast(pl.Utf8)
            .str.slice(1)
            .str.strptime(pl.Date, format="%Y%m%d")
            .alias("service_date"),
            pl.col("BLOCK_ABBR").cast(pl.String).alias("block_id"),
            pl.col("RUN_DESIGNATOR").cast(pl.String).alias("run_id"),
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
            pl.col("ONBOARD_LOGON_ID")
            .cast(pl.String)
            .alias("operator_badge_number"),
            pl.col("PROPERTY_TAG").cast(pl.String).alias("vehicle_label"),
            pl.col("ACTUAL_LOGON_TIME").alias("logon_sam"),
            pl.col("ACTUAL_LOGOFF_TIME").alias("logoff_sam"),
        )
        .with_columns(
            create_dt_from_sam(
                pl.col("service_date"), pl.col("logon_sam")
            ).alias("logon_time"),
            create_dt_from_sam(
                pl.col("service_date"), pl.col("logoff_sam")
            ).alias("logoff_time"),
        )
        .collect()
    )
