import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import datetime

    return datetime, pl


@app.cell
def _():
    from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule
    from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
    from lamp_py.utils.gtfs_utils import gtfs_from_parquet

    return bus_gtfs_schedule_events_for_date, generate_tm_schedule


@app.cell
def _():
    return


@app.cell
def _():

    from lamp_py.runtime_utils.remote_files import (
        tm_geo_node_file,
        tm_route_file,
        tm_trip_file,
        tm_vehicle_file,
        tm_time_point_file,
        tm_pattern_geo_node_xref_file,
    )

    return (
        tm_geo_node_file,
        tm_pattern_geo_node_xref_file,
        tm_time_point_file,
        tm_trip_file,
    )


@app.cell
def _(gtfs, pl, tm):

    # filter tm on trip ids that are in the gtfs set - tm has all trip ids ever, gtfs only has the ids scheduled for a single days
    tm_schedule = tm.tm_schedule.collect().filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs["plan_trip_id"].unique())
    )
    gtfs2 = gtfs.rename({"plan_trip_id": "trip_id"})

    # breakpoint()

    # gtfs has extra trips that tm doesn't, but tm should not have ANY
    # scheduled trips that are not in gtfs -
    combined_schedule = (
        gtfs2.join(tm_schedule, on=["trip_id", "stop_id"], how="full", coalesce=True)
        .join(
            tm.tm_pattern_geo_node_xref.select(["PATTERN_ID", "timepoint_order"]).collect(),
            on="PATTERN_ID",
            how="left",
            coalesce=True,
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
                    "PATTERN_ID",
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
            .when(pl.col("timepoint_order").is_null())  # ?
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
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
                "plan_travel_time_seconds",
                "plan_route_direction_headway_seconds",
                "plan_direction_destination_headway_seconds",
                "tm_joined",
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
        .sort(["trip_id", "tm_stop_sequence", "stop_sequence"])
    )

    return gtfs2, tm_schedule


@app.cell
def _(generate_tm_schedule):
    tm = generate_tm_schedule()
    return (tm,)


@app.cell
def _(tm):
    tm
    return


@app.cell
def _(pl, tm_pattern_geo_node_xref_file):
    tm_pattern_geo_node_xref = (
        (
            pl.scan_parquet(tm_pattern_geo_node_xref_file.s3_uri)
            .select("PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID", "GEO_NODE_ID")
            .filter(
                pl.col("TIME_POINT_ID").is_not_null()
                & pl.col("GEO_NODE_ID").is_not_null()
                & pl.col("PATTERN_ID").is_not_null()
                & pl.col("PATTERN_GEO_NODE_SEQ").is_not_null()
            )
        )
        .with_columns(
            pl.col(["PATTERN_GEO_NODE_SEQ"]).rank(method="dense").over(["PATTERN_ID"]).alias("timepoint_order"),
        )
        .collect()
    )
    return (tm_pattern_geo_node_xref,)


@app.cell
def _(pl, tm_geo_node_file):
    tm_geo_nodes = (
        pl.scan_parquet(tm_geo_node_file.s3_uri)
        .select(
            "GEO_NODE_ID",
            "GEO_NODE_ABBR",
        )
        .filter(pl.col("GEO_NODE_ABBR").is_not_null())
        .unique()
    ).collect()
    return (tm_geo_nodes,)


@app.cell
def _(pl, tm_trip_file):
    tm_trips = (
        pl.scan_parquet(tm_trip_file.s3_uri)
        .select(
            "TRIP_ID",
            "TRIP_SERIAL_NUMBER",
            "Pattern_ID",
        )
        .rename({"Pattern_ID": "PATTERN_ID"})
        .filter(pl.col("TRIP_SERIAL_NUMBER").is_not_null() & pl.col("PATTERN_ID").is_not_null())
        #          "TRIP_SERIAL_NUMBER": "trip_id"})
        .unique()
    ).collect()
    return (tm_trips,)


@app.cell
def _(pl, tm_time_point_file):
    tm_time_points = (
        pl.scan_parquet(tm_time_point_file.s3_uri).select(
            "TIME_POINT_ID",
            "TIME_POINT_ABBR",
            "TIME_PT_NAME",
        )
        # .rename({"TIME_POINT_ID": "time_point_id",
        #          "TIME_POINT_ABBR": "time_point_abbr",
        #          "TIME_PT_NAME" : "time_pt_name"})
    ).collect()
    return (tm_time_points,)


@app.cell
def _(tm_geo_nodes, tm_pattern_geo_node_xref_full, tm_time_points, tm_trips):
    tm_trip_geo_tp = (
        tm_trips.join(
            tm_pattern_geo_node_xref_full,
            on="PATTERN_ID",
            how="left",
            coalesce=True,
        )
        .join(tm_geo_nodes, on="GEO_NODE_ID", how="left", coalesce=True)
        .join(tm_time_points, on="TIME_POINT_ID", how="left", coalesce=True)
    )
    return (tm_trip_geo_tp,)


@app.cell
def _(gtfs, pl, tm_trip_geo_tp):
    tm_sequences = tm_trip_geo_tp.group_by(["TRIP_ID"]).agg(
        pl.col("PATTERN_GEO_NODE_SEQ").max().alias("tm_planned_sequence_end"),
        pl.col("PATTERN_GEO_NODE_SEQ").min().alias("tm_planned_sequence_start"),
    )

    tm_schedule = tm_trip_geo_tp.with_columns(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"), pl.col("GEO_NODE_ABBR").alias("stop_id")
    ).join(
        tm_sequences,
        on="TRIP_ID",
        how="left",
        coalesce=True,
    )

    tm_schedule = tm_schedule.filter(pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs["plan_trip_id"].unique()))
    return (tm_schedule,)


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(gtfs):
    gtfs2 = gtfs.rename({"plan_trip_id": "trip_id"})

    return (gtfs2,)


@app.cell
def _(abc):
    abc
    return


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["TRIP_ID", "stop_id", "PATTERN_GEO_NODE_SEQ"]).unique().height  # trip id unique
    tm_schedule.select(["trip_id", "stop_id", "PATTERN_GEO_NODE_SEQ"]).unique().height  # trip serial number not unique
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["trip_id", "PATTERN_GEO_NODE_SEQ"]).unique().height
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["trip_id", "stop_id", "PATTERN_GEO_NODE_SEQ"]).height
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["trip_id", "stop_id"])
    return


@app.cell
def _(tm_schedule):
    assert (
        tm_schedule.select(["trip_id", "stop_id", "PATTERN_GEO_NODE_SEQ"]).height
        == tm_schedule.select(["trip_id", "stop_id"]).height
    )
    return


@app.cell
def _(tm_schedule):
    tm_tid_sid_psq = tm_schedule.select(["trip_id", "stop_id", "PATTERN_GEO_NODE_SEQ"])
    return


@app.cell
def _(gtfs2):
    assert gtfs2.select(["trip_id", "stop_id", "stop_sequence"]).height == gtfs2.select(["trip_id", "stop_id"]).height
    return


@app.cell
def _(gtfs2):
    gtfs2.select(["trip_id", "stop_id"])
    return


@app.cell
def _(tm_schedule):
    tm_tid_sid = tm_schedule.select(["trip_id", "stop_id"])
    return


@app.cell
def _(gtfs2):
    gtfs2.select(["trip_id", "stop_id", "stop_sequence"]).unique()
    return


@app.cell
def _(gtfs2):
    gtfs2
    return


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(gtfs2, pl, tm, tm_schedule):
    abc = (
        gtfs2.join(tm_schedule, on=["trip_id", "stop_id"], how="full", coalesce=True)
        .join(
            tm.tm_pattern_geo_node_xref.collect(),
            on=["PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID"],
            how="left",
            coalesce=True,
        )
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
                    "PATTERN_ID",
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
            .when(pl.col("timepoint_order").is_null())  # ?
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
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
                "plan_travel_time_seconds",
                "plan_route_direction_headway_seconds",
                "plan_direction_destination_headway_seconds",
                "tm_joined",
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
        .sort(["trip_id", "tm_stop_sequence", "stop_sequence"])
    )
    return (abc,)


@app.cell
def _(abc, pl):
    single_abc = abc.filter(pl.col("trip_id") == "69112086")
    single_abc
    return


@app.cell
def _():
    # find the trips that stop at the same stop multiple times and need the stop_Sequence
    # look at those ones - see if this fixes issues there
    return


@app.cell
def _(abc, pl):
    # all routes with a null in tm_stop_sequence - why?
    # shuttles - ok
    # 716/714 - contracted?
    # 47?
    abc.filter(pl.col.tm_stop_sequence.is_null())["route_id"].unique()
    return


@app.cell
def _(abc, pl):
    abc.filter(pl.col.tm_stop_sequence.is_null(), pl.col.route_id == "47")
    return


@app.cell
def _(abc, pl):
    abc.filter(pl.col.trip_id == "69698912")
    return


@app.cell
def _(pl, tm_schedule):
    tm_schedule.filter(pl.col.TRIP_SERIAL_NUMBER == 69698912).sort("PATTERN_GEO_NODE_SEQ")
    return


@app.cell
def _(abc):
    abc.select(["trip_id", "stop_id", "stop_sequence"]).unique()
    return


@app.cell
def _(abc2):
    abc2
    return


@app.cell
def _(abc2):
    abc2.select(["trip_id", "stop_id", "stop_sequence"]).unique()
    return


@app.cell
def _(gtfs3):
    gtfs3
    return


@app.cell
def _(abc, pl):
    abc.filter(pl.col("route_id") == "741")
    return


@app.cell
def _(abc, pl):
    single_silver = abc.filter(pl.col("trip_id") == "69112086")
    single_silver
    return


@app.cell
def _(gtfs2):
    gtfs3 = gtfs2.sort(by="stop_sequence").set_sorted("stop_sequence")
    return (gtfs3,)


app._unparsable_cell(
    r"""
    gtfs2.sort(by=\"stop_sequence\").
    """,
    name="_",
)


@app.cell
def _(gtfs3, pl, tm, tm_schedule):
    # can't do join asof because that won't give us the "non-revenue" timepoints that we're aiming for
    abc2 = (
        gtfs3.join_asof(
            tm_schedule.sort(by="PATTERN_GEO_NODE_SEQ"),
            left_on="stop_sequence",
            right_on="PATTERN_GEO_NODE_SEQ",
            by=["trip_id", "stop_id"],
            strategy="nearest",
            coalesce=True,
        )
        .join(
            tm.tm_pattern_geo_node_xref.collect(),
            on=["PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID"],
            how="left",
            coalesce=True,
        )
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
                    "PATTERN_ID",
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
            .when(pl.col("timepoint_order").is_null())  # ?
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
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
                "plan_travel_time_seconds",
                "plan_route_direction_headway_seconds",
                "plan_direction_destination_headway_seconds",
                "tm_joined",
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
        .sort(["trip_id", "tm_stop_sequence", "stop_sequence"])
    )
    return (abc2,)


@app.cell
def _(abc2, pl):
    single_abc2 = abc2.filter(pl.col("trip_id") == "69112086")
    single_abc2
    return


@app.cell
def _(abc2):
    abc2
    return


@app.cell
def _(abc):
    abc.describe()
    return


@app.cell
def _(abc2):
    abc2.describe()
    return


@app.cell
def _(abc3):
    abc3.describe()
    return


@app.cell
def _(abc2, pl):
    abc3 = abc2.with_columns(
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
                "PATTERN_ID",
                # "plan_travel_time_seconds",
                # "plan_route_direction_headway_seconds",
                # "plan_direction_destination_headway_seconds"
            ]
        )
        .fill_null(strategy="forward")  # handle added non-rev stops that are at the beginning
        .fill_null(strategy="backward")  # handle added non-rev stops that are at the end
        .over(["trip_id"])
    )
    #     # add a column describing what data was used to form it.
    #     # to form the original datasets -
    #     # TM + JOIN = TM
    #     # GTFS + JOIN = GTFS
    #     .with_columns(
    #         pl.when(pl.col("stop_sequence").is_null())
    #         .then(pl.lit("TM"))
    #         .when(pl.col("timepoint_order").is_null())  # ?
    #         .then(pl.lit("GTFS"))
    #         .otherwise(pl.lit("JOIN"))
    #         .alias("tm_joined")
    #     )
    #     .with_columns(
    #         (
    #             pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
    #             pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
    #             pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
    #             pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
    #             pl.col("PATTERN_ID").cast(pl.Int64).alias("pattern_id"),
    #         )
    #     )
    #     # explicitly define the columns that we are grabbing at the end of the operation
    #     .select(
    #         [
    #             "trip_id",
    #             "stop_id",
    #             "stop_sequence",
    #             "block_id",
    #             "route_id",
    #             "service_id",
    #             "route_pattern_id",
    #             "route_pattern_typicality",
    #             "direction_id",
    #             "direction",
    #             "direction_destination",
    #             "stop_name",
    #             "plan_stop_count",
    #             "plan_start_time",
    #             "plan_start_dt",
    #             "plan_travel_time_seconds",
    #             "plan_route_direction_headway_seconds",
    #             "plan_direction_destination_headway_seconds",
    #             "tm_joined",
    #             "timepoint_order",
    #             "tm_stop_sequence",
    #             "tm_planned_sequence_start",
    #             "tm_planned_sequence_end",
    #             "timepoint_id",
    #             "timepoint_abbr",
    #             "timepoint_name",
    #             "pattern_id",
    #         ]
    #     )
    #     .sort(["trip_id", "tm_stop_sequence", "stop_sequence"])
    # )

    return (abc3,)


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(tm_pattern_geo_node_xref):
    tm_pattern_geo_node_xref
    return


app._unparsable_cell(
    r"""
    single1 = abc.filter(pl.col.trip_id == \"69363363\")
    single1.
    """,
    name="_",
)


@app.cell
def _(tm_pattern_geo_node_xref):
    tm_pattern_geo_node_xref
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(tm_trip_geo_tp):
    tm_trip_geo_tp
    return


@app.cell
def _(tm_trips):
    tm_trips
    return


@app.cell
def _(pl, tm_pattern_geo_node_xref_file):
    tm_pattern_geo_node_xref_full = (
        pl.scan_parquet(tm_pattern_geo_node_xref_file.s3_uri).select(
            "PATTERN_ID", "PATTERN_GEO_NODE_SEQ", "TIME_POINT_ID", "GEO_NODE_ID"
        )
    ).collect()
    return (tm_pattern_geo_node_xref_full,)


@app.cell
def _(tm_pattern_geo_node_xref_full):
    tm_pattern_geo_node_xref_full
    return


@app.cell
def _(bus_gtfs_schedule_events_for_date, datetime):
    service_date = datetime.date(year=2025, month=8, day=12)
    gtfs = bus_gtfs_schedule_events_for_date(datetime.date(year=2025, month=8, day=12))
    return (gtfs,)


@app.cell
def _(pl):
    sc = pl.read_parquet("s3://mbta-ctd-dataplatform-springboard/lamp/TM/STOP_CROSSING/120250626.parquet")
    return (sc,)


@app.cell
def _(sc):
    sc
    return


@app.cell
def _(pl, tm_trip_file):
    tm_trips2 = (
        pl.scan_parquet(tm_trip_file.s3_uri)
        .select(
            "TRIP_ID",
            "TRIP_SERIAL_NUMBER",
            "Pattern_ID",
            "WORK_PIECE_ID",
            "TRIP_SEQUENCE",
            "BLOCK_TRIP_SEQ",
            "TRIP_START_NODE_ID",
            "TRIP_END_NODE_ID",
            "TRIP_END_TIME",
        )
        .rename({"Pattern_ID": "PATTERN_ID"})
        .filter(pl.col("TRIP_SERIAL_NUMBER").is_not_null() & pl.col("PATTERN_ID").is_not_null())
        #          "TRIP_SERIAL_NUMBER": "trip_id"})
        .unique()
    ).collect()

    return (tm_trips2,)


@app.cell
def _(a, pl):
    a.filter(pl.col("TIME_POINT_ID") == 8356)
    return


@app.cell
def _(tm_trips2):
    tm_trips2.sort(["TRIP_SERIAL_NUMBER", "BLOCK_TRIP_SEQ"])
    return


@app.cell
def _(tm_trips2):
    tm_trips2["WORK_PIECE_ID"].is_null().all()
    return


@app.cell
def _(tm_trips):
    tm_trips
    return


@app.cell
def _(schedule_gtfs):
    schedule_gtfs2 = schedule_gtfs.rename({"plan_trip_id": "trip_id"})

    return (schedule_gtfs2,)


@app.cell
def _(schedule_gtfs2, tm_trips):
    schedule_gtfs2.join(tm_trips, on=["trip_id", "stop_id"], how="left", coalesce=True).sort(
        ["trip_id", "PATTERN_GEO_NODE_SEQ", "stop_sequence"]
    )
    return


@app.cell
def _():
    interesting_filter = [
        "68964139",
    ]
    68964139  # hinh
    return


@app.cell
def _(tm_trips):
    tm_trips
    return


@app.cell
def _(schedule_gtfs2):
    schedule_gtfs2
    return


@app.cell
def _(schedule_tm):
    schedule_tm.tm_trip_file
    return


@app.cell
def _(tm_trips):
    tm_trips
    return


@app.cell
def _():
    return


@app.cell
def _(pl, schedule_gtfs2, tm_trips):
    append_list = []
    for trip_id, single in schedule_gtfs2.group_by(by="trip_id"):
        # breakpoint()
        single_tm = tm_trips.filter(pl.col("trip_id") == trip_id[0]).sort("PATTERN_GEO_NODE_SEQ")
        # single = schedule_gtfs.filter(pl.col('plan_trip_id') == "68964139").sort(["stop_sequence"])
        # 69112229

        combined_left = (
            single.join(single_tm, left_on="stop_id", right_on="GEO_NODE_ABBR", how="full", coalesce=True)
            .sort(["PATTERN_GEO_NODE_SEQ", "stop_sequence"])
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
                        # "plan_travel_time_seconds", # i could fill these in or i can leave them null. depends on what the calcs like better
                        # "plan_route_direction_headway_seconds",
                        # "plan_direction_destination_headway_seconds"
                    ]
                )
                .fill_null(strategy="forward")
                .fill_null(strategy="backward")
            )
        )
        combined_left = combined_left.with_columns(
            pl.when(pl.col("stop_sequence").is_null())
            .then(pl.lit("TM"))
            .when(pl.col("timepoint_order").is_null())
            .then(pl.lit("GTFS"))
            .otherwise(pl.lit("JOIN"))
            .alias("tm_joined")
        )

        append_list.append(combined_left)
        # if combined_left['tm_joined'].str.contains("TM").any():
        #     break
        # breakpoint()
    return append_list, combined_left


@app.cell
def _(append_list, pl):
    output = pl.concat(append_list, how="vertical")
    output = output.with_columns(pl.col("plan_start_dt").dt.date().alias("service_date"))
    output = output.with_columns(
        # (pl.col("ROUTE_ABBR").cast(pl.String).str.strip_chars_start("0").alias("route_id")),
        # pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).alias("trip_id"),
        # pl.col("GEO_NODE_ABBR").cast(pl.String).alias("stop_id"),
        pl.col("PATTERN_GEO_NODE_SEQ").cast(pl.Int64).alias("tm_stop_sequence"),
        # pl.col("tm_planned_sequence_start"),
        # pl.col("tm_planned_sequence_end"),
        # pl.col("PROPERTY_TAG").cast(pl.String).alias("vehicle_label"),
        pl.col("TIME_POINT_ID").cast(pl.Int64).alias("timepoint_id"),
        pl.col("TIME_POINT_ABBR").cast(pl.String).alias("timepoint_abbr"),
        pl.col("TIME_PT_NAME").cast(pl.String).alias("timepoint_name"),
        pl.col("PATTERN_ID").cast(pl.Int64).alias("pattern_id"),
    ).select(
        [
            "service_date",
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
            "plan_travel_time_seconds",
            "plan_route_direction_headway_seconds",
            "plan_direction_destination_headway_seconds",
            "tm_joined",
            "timepoint_order",
            "tm_stop_sequence",
            "timepoint_id",
            "timepoint_abbr",
            "timepoint_name",
            "pattern_id",
        ]
    )

    # .drop([  "TRIP_ID",
    #   "TRIP_SERIAL_NUMBER",
    #   "PATTERN_ID",
    #   "PATTERN_GEO_NODE_SEQ",
    #   "TIME_POINT_ID",
    #   "GEO_NODE_ID",
    #   "TIME_POINT_ABBR",
    #   "TIME_PT_NAME",
    #   "trip_id_right",
    #   "stop_id_right",])

    # # [
    #   "trip_id",
    #   "stop_id",
    #   "stop_sequence",
    #   "block_id",
    #   "route_id",
    #   "service_id",
    #   "route_pattern_id",
    #   "route_pattern_typicality",
    #   "direction_id",
    #   "direction",
    #   "direction_destination",
    #   "stop_name",
    #   "plan_stop_count",
    #   "plan_start_time",
    #   "plan_start_dt",
    #   "plan_travel_time_seconds",
    #   "plan_route_direction_headway_seconds",
    #   "plan_direction_destination_headway_seconds",
    #   "TRIP_ID",
    #   "TRIP_SERIAL_NUMBER",
    #   "PATTERN_ID",
    #   "PATTERN_GEO_NODE_SEQ",
    #   "TIME_POINT_ID",
    #   "GEO_NODE_ID",
    #   "timepoint_order",
    #   "TIME_POINT_ABBR",
    #   "TIME_PT_NAME",
    #   "trip_id_right",
    #   "stop_id_right",
    #   "tm_joined",
    #   "service_date",
    #   "tm_stop_sequence",
    #   "timepoint_id",
    #   "timepoint_abbr",
    #   "timepoint_name",
    #   "pattern_id"
    # ]
    return (output,)


@app.cell
def _():
    return


@app.cell
def _(output):
    output.sort(["trip_id", "plan_start_dt", "stop_sequence", "tm_stop_sequence"])
    return


@app.cell
def _(output):
    for_print = output.sort(["trip_id", "plan_start_dt", "stop_sequence", "tm_stop_sequence"])
    for_print
    return


@app.cell
def _(output, pl):
    tids = output.filter(pl.col("tm_joined") == "TM").select("trip_id").unique()
    tids
    return (tids,)


@app.cell
def _(output, pl, tids):
    interesting = output.filter(pl.col("trip_id").is_in(tids["trip_id"])).sort(
        ["trip_id", "plan_start_dt", "stop_sequence", "tm_stop_sequence"]
    )
    return (interesting,)


@app.cell
def _(interesting):
    interesting
    return


@app.cell
def _(interesting, pl):
    interesting.filter(pl.col("trip_id") == "69898297")
    return


@app.cell
def _():
    return


@app.cell
def _():

    #     (
    #         (pl.col("service_date") + pl.duration(seconds="SCHEDULED_TIME"))
    #         .dt.replace_time_zone("America/New_York", ambiguous="earliest")
    #         .dt.convert_time_zone("UTC")
    #         .alias("tm_scheduled_time_dt")
    #     ),
    #     # (
    #     #     (pl.col("service_date") + pl.duration(seconds="ACT_ARRIVAL_TIME"))
    #     #     .dt.replace_time_zone("America/New_York", ambiguous="earliest")
    #     #     .dt.convert_time_zone("UTC")
    #     #     .alias("tm_actual_arrival_dt")
    #     # ),
    #     # (
    #     #     (pl.col("service_date") + pl.duration(seconds="ACT_DEPARTURE_TIME"))
    #     #     .dt.replace_time_zone("America/New_York", ambiguous="earliest")
    #     #     .dt.convert_time_zone("UTC")
    #     #     .alias("tm_actual_departure_dt")
    #     # ),
    #     pl.col("SCHEDULED_TIME").cast(pl.Int64).alias("tm_scheduled_time_sam"),
    #     # pl.col("ACT_ARRIVAL_TIME").cast(pl.Int64).alias("tm_actual_arrival_time_sam"),
    #     # pl.col("ACT_DEPARTURE_TIME").cast(pl.Int64).alias("tm_actual_departure_time_sam"),
    # )
    return


@app.cell
def _():
    return


@app.cell
def _(combined_left):
    combined_left.sort(["stop_sequence"])
    return


@app.cell
def _(combined_left):
    combined_left["tm_joined"].str.contains("TM").any()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
