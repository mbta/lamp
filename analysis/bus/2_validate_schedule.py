import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import polars as pl

    return (pl,)


@app.cell
def _(pl):
    gtfs_schedule = pl.read_parquet("gtfs_schedule.parquet")
    tm_schedule = pl.read_parquet("tm_schedule.parquet").filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs_schedule["plan_trip_id"].unique().implode())
    )

    return gtfs_schedule, tm_schedule


@app.cell
def _(pl):
    recent = pl.read_parquet("/tmp/20250813_20250815_bus_recent.parquet")
    return (recent,)


@app.cell
def _(recent):
    recent.describe()
    return


@app.cell
def _(recent):
    recent.columns
    return


@app.cell
def _(pl):
    combined_schedule = pl.read_parquet("combined_schedule.parquet")

    return (combined_schedule,)


@app.cell
def _(pl):
    gtfs = pl.read_parquet("gtfs_events.parquet")
    tm = pl.read_parquet("tm_events.parquet")
    return gtfs, tm


@app.cell
def _(gtfs):
    gtfs
    return


@app.cell
def _(gtfs_schedule_without_events, mo):
    mo.md(
        f"""Not all scheduled GTFS trips have events - {gtfs_schedule_without_events} scheduled trips without events"""
    )
    return


@app.cell
def _(gtfs_schedule, schedule_gtfs):
    gtfs_schedule_without_events = gtfs_schedule.height - schedule_gtfs.select("service_date").drop_nulls().height
    return (gtfs_schedule_without_events,)


@app.cell
def _(schedule_gtfs):
    schedule_gtfs.select("service_date").drop_nulls()
    return


@app.cell
def _(pl, schedule_gtfs):
    schedule_gtfs.filter(pl.col("service_date").is_not_null()).with_columns(
        (pl.col("direction_id") == pl.col("direction_id_right_gtfs")).alias("same")
    ).select("same")
    return


@app.cell
def _(schedule_gtfs):
    schedule_gtfs.select("direction_id", "direction_id_right_gtfs")
    return


@app.cell
def _(schedule_gtfs):
    schedule_gtfs.height
    return


@app.cell
def _(schedule_gtfs_tm):
    schedule_gtfs_tm.columns
    return


@app.cell
def _(combined_schedule, gtfs, tm):

    schedule_gtfs = combined_schedule.sort(by="stop_sequence").join_asof(
        gtfs.sort(by="stop_sequence"),
        on="stop_sequence",
        by=["trip_id", "stop_id", "direction_id", "route_id"],  # direction id and route id are only here
        strategy="nearest",
        coalesce=True,
        suffix="_right_gtfs",
    )  # .drop("direction_id_right_gtfs", "route_id_right_gtfs")

    schedule_gtfs_tm = (
        schedule_gtfs.sort(by="tm_stop_sequence")
        .join_asof(
            tm.sort(by="tm_stop_sequence"),
            on="tm_stop_sequence",
            by=["trip_id", "stop_id"],
            strategy="nearest",
            coalesce=True,
            suffix="_right_tm",
        )
        .drop(
            "route_id_right_tm",
            "timepoint_order_right_tm",
            "tm_planned_sequence_start_right_tm",
            "tm_planned_sequence_end_right_tm",
            "vehicle_label_right_tm",
            "timepoint_id_right_tm",
            "timepoint_abbr_right_tm",
            "timepoint_name_right_tm",
            "pattern_id_right_tm",
        )
    )
    return schedule_gtfs, schedule_gtfs_tm


@app.cell
def _(combined_schedule):
    combined_schedule.columns
    return


@app.cell
def _(pl):
    trips = pl.read_parquet("trips_with_issues.parquet")
    return (trips,)


@app.cell
def _(trips):
    trips
    return


@app.cell
def _(pl, trips):
    trips_next = trips
    trips_next = trips_next.filter(pl.col("trip_id") == "70016201")
    return


@app.cell
def _(mo, trips):
    mo.ui.dataframe(trips)
    return


@app.cell
def _(combined_schedule):
    combined_schedule
    return


@app.cell
def _():
    return


@app.cell
def _(combined_schedule):
    combined_schedule
    return


@app.cell
def _(gtfs_schedule):
    gtfs_schedule

    return


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(combined_schedule):
    combined_schedule
    return


@app.cell
def _(pl, tm_schedule):
    tm1 = tm_schedule.filter(pl.col.TRIP_ID == 8660115)
    return (tm1,)


@app.cell
def _(pl, tm_schedule):
    tm2 = tm_schedule.filter(pl.col.TRIP_ID == 8690092)
    tm2
    return


@app.cell
def _(gtfs_schedule, pl):
    gg = gtfs_schedule.filter(pl.col("plan_trip_id") == "70040140").sort("stop_sequence")
    return (gg,)


@app.cell
def _(gg):
    gg
    return


@app.cell
def _(pl, tm_schedule):
    tt = tm_schedule.filter(pl.col("TRIP_SERIAL_NUMBER") == 70040140).sort("PATTERN_GEO_NODE_SEQ")
    return (tt,)


@app.cell
def _(tt):
    tt
    return


@app.cell
def _(gg, tt):
    gg.join_asof(
        tt,
        left_on="stop_sequence",
        right_on="PATTERN_GEO_NODE_SEQ",
        by_left=["plan_trip_id", "stop_id"],
        by_right=["trip_id", "GEO_NODE_ABBR"],
        strategy="nearest",
        coalesce=True,
    )
    return


@app.cell
def _(combined_schedule, pl):
    weird = combined_schedule.filter(pl.col("trip_id") == "70040140").filter(pl.col.stop_id == "4023").unique()
    return (weird,)


@app.cell
def _(pl, weird):
    weird.filter(pl.when(pl.col("stop_sequence") - pl.col("tm_stop_sequence") > 0))
    return


@app.cell
def _(gtfs2):
    gtfs2
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["TRIP_SERIAL_NUMBER", "GEO_NODE_ABBR"])
    return


@app.cell
def _(tm_schedule):
    tm_schedule.select(["TRIP_SERIAL_NUMBER", "GEO_NODE_ID"]).unique()
    return


@app.cell
def _(pl, tm_schedule):
    tm3 = tm_schedule.filter(pl.col.TRIP_SERIAL_NUMBER == 70040140).sort(by="PATTERN_GEO_NODE_SEQ")
    return (tm3,)


@app.cell
def _(tm3):
    tm3.select(["TRIP_SERIAL_NUMBER", "GEO_NODE_ABBR"]).unique()
    return


@app.cell
def _(tm_schedule):
    tm_schedule
    return


@app.cell
def _(pl, tm_schedule):
    tm_schedule.filter(pl.col.trip_id == "70040149").sort("PATTERN_GEO_NODE_SEQ")
    return


@app.cell
def _(gtfs_schedule, tm_schedule):
    gtfs2 = gtfs_schedule.rename({"plan_trip_id": "trip_id"})

    non_rev_points = tm_schedule.join(gtfs2, on=["trip_id", "stop_id"], how="anti", coalesce=True)

    return gtfs2, non_rev_points


@app.cell
def _(non_rev_points):
    non_rev_points
    return


@app.cell
def _(gtfs_schedule):
    gtfs_schedule.height
    return


@app.cell
def _(combined_schedule):
    combined_schedule
    return


@app.cell
def _(combined_schedule, gtfs_schedule, non_rev_points):
    combined_schedule.height - gtfs_schedule.height - non_rev_points.height
    return


@app.cell
def _(combined_schedule):
    combined_schedule.height - 365194
    return


@app.cell
def _(combined_schedule):
    combined_schedule.describe()
    return


@app.cell
def _(combined_schedule, pl):
    combined_schedule.filter(pl.col.trip_id == "70040149").sort("stop_sequence").filter(
        pl.col.stop_sequence.is_in([33, 36])
    )
    return


@app.cell
def _(pl):
    trip = pl.read_parquet("70040149.parquet")
    trip = trip.sort(["trip_id", "stop_sequence"]).with_columns(
        (pl.col("stop_sequence").shift(-1) - pl.col("stop_sequence")).alias("sequence_diff").over("trip_id"),
        (pl.col("tm_stop_sequence").shift(-1) - pl.col("tm_stop_sequence")).alias("tm_sequence_diff").over("trip_id"),
        (pl.col("stop_sequence") - pl.col("tm_stop_sequence")).alias("tm_gtfs_sequence_diff").abs(),
    )
    trip = trip.sort(["trip_id", "tm_stop_sequence"]).with_columns(
        (pl.col("tm_stop_sequence").shift(-1) - pl.col("tm_stop_sequence")).alias("tm_sequence_diff").over("trip_id"),
    )

    filter_out_gtfs2 = (pl.col.sequence_diff < 1) & (
        pl.col("tm_gtfs_sequence_diff").eq(pl.col("tm_gtfs_sequence_diff").max())
    ).over(["trip_id", "stop_sequence"])
    filter_out_tm2 = (pl.col.tm_sequence_diff < 1) & (
        pl.col("tm_gtfs_sequence_diff").eq(pl.col("tm_gtfs_sequence_diff").max())
    ).over(["trip_id", "tm_stop_sequence"])

    filter_out222 = trip.filter(filter_out_gtfs2 | filter_out_tm2)["index"].implode()
    return filter_out222, trip


@app.cell
def _(filter_out222):
    filter_out222
    return


@app.cell
def _(pl, trip):
    trip.filter(pl.col.stop_sequence.is_in([33, 36]))
    return


@app.cell
def _(pl, trip):
    trip.filter(pl.col.stop_sequence.is_in([33, 36])).sort("stop_sequence").filter(pl.col.stop_sequence == 33).filter(
        pl.col.sequence_diff == 0
    ).drop
    return


@app.cell
def _(combined_schedule, pl):
    cs = combined_schedule.sort(["trip_id", "stop_sequence"]).with_columns(
        (pl.col("stop_sequence").shift(-1) - pl.col("stop_sequence")).alias("sequence_diff").over("trip_id"),
        (pl.col("tm_stop_sequence").shift(-1) - pl.col("tm_stop_sequence")).alias("tm_sequence_diff").over("trip_id"),
        (pl.col("stop_sequence") - pl.col("tm_stop_sequence")).alias("tm_gtfs_sequence_diff"),
    )
    cs = cs.sort(["trip_id", "tm_stop_sequence"]).with_columns(
        (pl.col("tm_stop_sequence").shift(-1) - pl.col("tm_stop_sequence")).alias("tm_sequence_diff").over("trip_id"),
    )
    # cs = cs.with_row_index()
    # tm_schedule.select(['TRIP_SERIAL_NUMBER', 'GEO_NODE_ABBR'])
    return (cs,)


@app.cell
def _(hmm):
    hmm
    return


@app.cell
def _(cs):
    cs
    return


@app.cell
def _(cs, pl):
    tmp = cs.filter((pl.col.sequence_diff < 1))
    filter_out = tmp.filter(pl.col("tm_gtfs_sequence_diff").eq(pl.col("tm_gtfs_sequence_diff").max()).over("trip_id"))[
        "index"
    ].implode()
    return (filter_out,)


@app.cell
def _(cs, pl):
    cs.filter(pl.col.trip_id == "70040149")
    return


@app.cell
def _(combined_schedule, pl):
    combined_schedule.filter(pl.col.trip_id == "70040149")
    return


@app.cell
def _(cs, pl):
    # cs.filter((pl.col.sequence_diff < 1) & (pl.col.tm_gtfs_sequence_diff == pl.col.tm_gtfs_sequence_diff.max()).over("trip_id"))
    # tmp = cs.filter((pl.col.sequence_diff < 1))

    filter_out_gtfs = (pl.col.sequence_diff < 1) & pl.col("tm_gtfs_sequence_diff").eq(
        pl.col("tm_gtfs_sequence_diff").max()
    ).over("trip_id")
    filter_out_tm = (pl.col.tm_sequence_diff < 1) & pl.col("tm_gtfs_sequence_diff").eq(
        pl.col("tm_gtfs_sequence_diff").min()
    ).over("trip_id")

    filter_out11 = cs.filter(filter_out_gtfs)["index"]
    filter_out22 = cs.filter(filter_out_tm)["index"]

    filter_out = cs.filter(filter_out_gtfs | filter_out_tm)["index"].implode()
    # filter_out.extend(filter_out2)
    return filter_out, filter_out22, filter_out_gtfs, filter_out_tm


@app.cell
def _(combined_schedule, filter_out_gtfs, filter_out_tm):
    combined_schedule.filter(filter_out_gtfs | filter_out_tm)

    return


@app.cell
def _(filter_out22):
    filter_out22
    return


@app.cell
def _(filter_out):
    filter_out[0]
    return


@app.cell
def _(cs, filter_out, pl):
    cs2 = cs.filter(~pl.col("index").is_in(filter_out)).drop()

    return (cs2,)


@app.cell
def _(cs2):
    cs2
    return


@app.cell
def _(cs2, pl):

    # TM
    tmp2 = cs2.filter((pl.col.tm_sequence_diff < 1))
    tmp2
    filter_out2 = tmp2.filter(
        (
            (pl.col.tm_sequence_diff < 1)
            & pl.col("tm_gtfs_sequence_diff").eq(pl.col("tm_gtfs_sequence_diff").max()).over("trip_id")
        )
    )["index"].implode()
    return filter_out2, tmp2


@app.cell
def _(tmp2):
    tmp2
    return


@app.cell
def _(filter_out2):
    filter_out2
    return


@app.cell
def _(cs, cs2):
    assert cs.height - cs2.height == 52
    return


@app.cell
def _():
    return


@app.cell
def _(tm1):
    tm1["PATTERN_GEO_NODE_SEQ"].unique().len()
    return


@app.cell
def _(tm1):
    stats = tm1.describe()
    return (stats,)


@app.cell
def _(gtfs_schedule):
    gtfs_schedule
    return


@app.cell
def _(pl, tm1, tm_schedule):
    def _():
        incrementing = [
            "PATTERN_GEO_NODE_SEQ",
            "TIME_POINT_ID",
            "GEO_NODE_ID",
            "GEO_NODE_ABBR",
            "TIME_POINT_ABBR",
            "TIME_PT_NAME",
            "stop_id",
        ]
        static_cols_tm = [
            "TRIP_ID",
            "TRIP_SERIAL_NUMBER",
            "PATTERN_ID",
            "trip_id",
            "tm_planned_sequence_end",
            "tm_planned_sequence_start",
        ]

        for idx, tm in tm_schedule.group_by("TRIP_ID"):
            stats = tm1.describe()

            for col in static_cols_tm:
                assert (
                    stats.row(by_predicate=pl.col("statistic") == "min")[stats.get_column_index(name=col)]
                    == stats.row(by_predicate=pl.col("statistic") == "max")[stats.get_column_index(name=col)]
                )

    # _()
    return


@app.cell
def _(stats):
    stats
    return


@app.cell
def _(combined_schedule):
    combined_schedule.columns
    return


@app.cell
def _(gtfs_schedule):
    gtfs_schedule.columns
    return


@app.cell
def _(stats):
    stats.get_column_index(name="TRIP_ID")
    return


@app.cell
def _():
    return


@app.cell
def _(pl, stats):
    stats.row(by_predicate=pl.col("statistic") == "min")[stats.get_column_index(name="TRIP_ID")] == stats.row(
        by_predicate=pl.col("statistic") == "max"
    )[stats.get_column_index(name="TRIP_ID")]
    return


if __name__ == "__main__":
    app.run()
