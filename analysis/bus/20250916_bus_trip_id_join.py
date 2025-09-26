import marimo

__generated_with = "0.14.16"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pyarrow
    import pyarrow.parquet as pq
    import pyarrow.dataset as pd
    import pyarrow.compute as pc

    from pyarrow.fs import S3FileSystem
    import os
    from lamp_py.runtime_utils.remote_files import rt_alerts
    from lamp_py.aws.s3 import (
        file_list_from_s3,
        file_list_from_s3_with_details,
        object_exists,
        file_list_from_s3_date_range,
    )
    from datetime import datetime, timedelta, date
    from dataclasses import dataclass, field

    return dataclass, date, datetime, field, mo, pl, timedelta


@app.cell
def _():
    from lamp_py.bus_performance_manager.combined_bus_schedule import (
        join_tm_schedule_to_gtfs_schedule,
        CombinedSchedule,
    )
    from lamp_py.bus_performance_manager.events_gtfs_rt import (
        generate_gtfs_rt_events,
        GTFSEvents,
        remove_overload_and_special_route_suffix,
    )
    from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
    from lamp_py.bus_performance_manager.events_tm import generate_tm_events, TransitMasterEvents
    from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
    from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule

    return (generate_gtfs_rt_events,)


@app.cell
def _():
    return


@app.cell
def _():
    # end_date = datetime(year=2025, month=6, day=12)
    # start_date = end_date - timedelta(days=0)
    prefix = "20250916_bus_trip_id_join__20250612"
    return


@app.cell
def _():
    # for each busdata:

    # get all the modified _1/_2/_OL

    # get the base trip_id

    # filter tm on trip_id

    # filter gtfs on contains trip_id

    # group_by trip_id, trip_id_suffix, vehicle

    # ensure stop
    return


@app.cell
def _():
    # run_bus_events(start_date=start_date, end_date=end_date, prefix=prefix)
    return


@app.cell
def _(dataclass, field, pl):

    @dataclass
    class BusData:
        bus_events: pl.DataFrame = field(init=False)
        tm_event: pl.DataFrame = field(init=False)
        gtfs_event: pl.DataFrame = field(init=False)
        tm_sched: pl.DataFrame = field(init=False)
        gtfs_sched: pl.DataFrame = field(init=False)
        combined_schedule: pl.DataFrame = field(init=False)

    def load_all(path: str, df_name):

        busdata = BusData()
        busdata.bus_events = pl.read_parquet(f"{path}/{df_name}")
        busdata.tm_event = pl.read_parquet(f"{path}/tm_events.parquet")
        busdata.gtfs_event = pl.read_parquet(f"{path}/gtfs_events.parquet")
        busdata.tm_sched = pl.read_parquet(f"{path}/tm_schedule.parquet")
        busdata.gtfs_sched = pl.read_parquet(f"{path}/gtfs_schedule.parquet")
        busdata.combined_schedule = pl.read_parquet(f"{path}/combined_schedule.parquet")

        return busdata

    og = load_all(path="/tmp/og/", df_name="OG_20250612_bus_recent.parquet")
    new = load_all(path="/tmp/new/", df_name="CLEAN_bus_recent.parquet")
    return BusData, new, og


@app.cell
def _(og):
    og.bus_events.columns
    return


@app.cell
def _(og):
    og.gtfs_event["trip_id"].str.contains("OL").sum()
    return


@app.cell
def _(og):
    og.gtfs_event["trip_id"].str.contains("_1").sum() + og.gtfs_event["trip_id"].str.contains("_2").sum()
    return


@app.cell
def _(og, pl):
    og.gtfs_event.filter(
        pl.col("trip_id").str.contains("OL")
        | pl.col("trip_id").str.contains("_1")
        | pl.col("trip_id").str.contains("_2")
    )["trip_id"].unique()
    return


@app.cell
def _():
    # 68162195
    # 3218
    return


@app.cell
def _(all_1_2, all_ol, pl):
    pl.concat([all_1_2, all_ol]).str.contains("68161373")
    return


@app.cell
def _(all_1_2, all_ol, pl):
    pl.concat([all_1_2, all_ol]).str.replace("_.*", "").str.replace("-OL.*", "").unique().to_list()
    return


@app.cell
def _():
    return


@app.cell
def _():
    import plotly.express as px

    def plot_lla(vp, hover=None):
        fig3 = px.scatter_map(vp, lat="latitude", lon="longitude", zoom=8, height=300, hover_data=hover)
        fig3.update_layout(mapbox_style="open-street-map")
        fig3.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        config2 = {"scrollZoom": True}
        return fig3.show(config2)

    return (plot_lla,)


@app.cell
def _():
    return


@app.cell
def _(pl):
    eg = pl.read_parquet("/Users/hhuang/lamp/lamp2/68161373.parquet")
    return (eg,)


@app.cell
def _(pl):
    gtfss = pl.read_parquet("/tmp/gtfs_schedule.parquet")
    return (gtfss,)


@app.cell
def _(mo):
    mo.md(
        r"""
    switching it up, to try to save the trip_id messing till the last possible moment. If i do nothing until join_rt_to_schedule...


    _1, _2 - doesn't have overloads
    
    gtfs_schedule = bus_gtfs_schedule_events_for_date(service_date)

        # no OL, no _1, _2 trips??
        tm_schedule = generate_tm_schedule()

        # full join results in _1, _2, all TM, all GTFS
        combined_schedule = join_tm_schedule_to_gtfs_schedule(gtfs_schedule, tm_schedule, **debug_flags)

    lets check up to this point...

    ```
    valid['trip_id'].str.contains('_1').sum()
    433
    valid['trip_id'].str.contains('_2').sum()
    472
    ```

    as expected. 


        # _1, _2, -OL1, -OL2
        gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)
        # transit master events from parquet

    14 invalid reported - asdfadsf = pl.read_parquet('68245041-OL2.parquet') - TODO come back and print invalid
    skipping these, concat valid and invalid to continue on

        # _1, _2 without suffix, -OL without suffix. 
        tm_df = generate_tm_events(tm_files, tm_schedule)

    no issues


    '68245041-OL2.parquet'


    ```
    tm_df['trip_id'].unique().is_in(
    [
      "68165414",
      "68245024",
      "68689654",
      "68690025",
      "68689261",
      "68689256",
      "68244977",
      "68245019",
      "68245188",
      "68245027",
      "68244978",
      "68245218",
      "68245071",
      "68245047",
      "68245015",
      "68165413",
      "68245041",
      "68689993",
      "68689266",
      "68245196",
      "68689608",
      "68690141",
      "68245011",
      "68245048",
      "68245092",
      "68244926",
      "68662284",
      "68689263",
      "68245025",
      "68689989",
      "68689253",
      "68245008",
      "68245195",
      "68163085",
      "68162195",
      "68662280",
      "68245100",
      "68245052",
      "68162572",
      "68245049",
      "68245216",
      "68244947",
      "68690136",
      "68245190",
      "68689984",
      "68689260",
      "68245078",
      "68245334",
      "68245202",
      "68689136",
      "68688855",
      "68161373",
      "68244982",
      "68689265",
      "68690144",
      "68162531",
      "68162568",
      "68245030",
      "68690008",
      "68245094",
      "68689109",
      "68689259",
      "68244998"
    ]
    ).sum() 
    # 63 - all of them in, confirmed
    ```


    gtfs_events_processed = gtfs_events.with_columns(
            pl.col("trip_id").alias("trip_id_gtfs"),
            pl.col("trip_id").str.replace(r"-OL\d?", "")
        )

    ```
    gtfs['trip_id'].str.contains('-OL1').sum()
    236
    gtfs['trip_id'].str.contains('-OL2').sum()
    36    

    schedule_vehicles = schedule.join(
            pl.concat(
                [gtfs.with_columns(pl.col("trip_id").str.replace(r"-OL\d?", "")).select("trip_id", "vehicle_label", "stop_id")]
            ).unique(),
            how="left",
            on=["trip_id", "stop_id"],
            coalesce=True,
        )
    ```


    now right with the vehicle_label forward/backward fill, now do _1 and _2 in preparation for joining TM events
    `.with_columns(pl.col('trip_id').str.replace(r"_\d", ""))`

    result:
    ```
    schedule_gtfs['trip_id'].str.contains('_1').any()
    False
    schedule_gtfs['trip_id'].str.contains('_2').any()
    False
    schedule_gtfs['trip_id'].str.contains('-OL').any()
    False
    ```


    looking at the vehicle_label fill now. we're getting vehicl leaking in that doesn't seem right. compare it to gtfs schedule, gtfs events, tm, events, this is fake
    ```
    df68245216 = pl.read_parquet('68245216.parquet')
    df68245216.sort(by=["vehicle_label", "trip_id_gtfs", "tm_stop_sequence"]).select("trip_id_gtfs", "tm_stop_sequence", "stop_sequence", "timepoint_order", "plan_stop_departure_dt", "vehicle_label", "tm_actual_departure_dt")
    df68245216.sort(by=["vehicle_label", "trip_id_gtfs", "tm_stop_sequence"])
    ```
    """
    )
    return


@app.cell
def _(pl):
    asdfadsf = pl.read_parquet("68245041-OL2.parquet")
    return (asdfadsf,)


@app.cell
def _(asdfadsf):
    asdfadsf.sort(by="gtfs_travel_to_dt")
    return


@app.cell
def _(gtfss, pl):
    gtfss.filter(pl.col.plan_trip_id.str.contains("68161373")).filter(pl.col.stop_sequence == 29)
    return


@app.cell
def _(pl):
    ge = pl.read_parquet("gtfs_events_68161373.parquet")
    ge.sort("stop_sequence")
    return


@app.cell
def _(eg):
    eg.sort("tm_stop_sequence")
    return


@app.cell
def _(eg, plot_lla):
    plot_lla(eg)
    return


@app.cell
def _(eg):
    eg
    return


@app.cell
def _(eg):
    eg
    return


@app.cell
def _(mo, og, pl):
    all_1_2 = og.gtfs_sched.filter(
        pl.col("plan_trip_id").str.contains("_1") | pl.col("plan_trip_id").str.contains("_2")
    )["plan_trip_id"].unique()
    any_1_2_in_tm_sched = (
        og.tm_sched["TRIP_SERIAL_NUMBER"].is_in(all_1_2.str.replace("_.*", "").cast(pl.Int64).implode()).any()
    )

    all_1_2_in_tm_events = all_1_2.str.replace("_.*", "").is_in(og.tm_event["trip_id"].unique().implode()).all()

    mo.md(
        f"""

    Are _1 _2 trip ids in TM schedule? {any_1_2_in_tm_sched}

    Are ALL _1 _2 trip ids in TM events? {all_1_2_in_tm_events}

    This might just be a staging artifact, because the tm_sched could be off
    """
    )
    return (all_1_2,)


@app.cell
def _(og, pl):
    og.gtfs_event.filter(pl.col("trip_id").str.contains("OL"))["trip_id"].unique().str.replace("-OL.*", "").is_in(
        og.tm_sched["trip_id"].unique().implode()
    ).all()
    return


@app.cell
def _(new):
    new.bus_events.columns
    return


@app.cell
def _(mo, og, pl):
    all_ol = og.gtfs_event.filter(pl.col("trip_id").str.contains("OL"))["trip_id"].unique()
    all_OL_in_tm_sched = all_ol.str.replace("-OL.*", "").is_in(og.tm_sched["trip_id"].unique().implode()).all()

    all_OL_tm_events = all_ol.str.replace("-OL.*", "").is_in(og.tm_event["trip_id"].unique().implode()).all()
    # all_1_2_in_tm_events = og.gtfs_sched.filter(pl.col('plan_trip_id').str.contains('_1') | pl.col('plan_trip_id').str.contains('_2'))['plan_trip_id'].unique().str.replace("_.*", "").is_in(og.tm_event['trip_id'].unique().implode()).all()

    mo.md(
        f"""

    Are ALL OL trip ids in TM schedule? {all_OL_in_tm_sched}

    Are ALL OL trip ids in TM events? {all_OL_tm_events}

    This might just be a staging artifact, because the tm_sched could be off
    """
    )
    return (all_ol,)


@app.cell
def _(mo, og, pl):
    all_1_2_in_gtfs_event_sched = (
        og.gtfs_event.filter(pl.col("trip_id").str.contains("_1") | pl.col("trip_id").str.contains("_2"))["trip_id"]
        .unique()
        .sort()
        == og.gtfs_sched.filter(pl.col("plan_trip_id").str.contains("_1") | pl.col("plan_trip_id").str.contains("_2"))[
            "plan_trip_id"
        ]
        .unique()
        .sort()
    ).all()

    mo.md(f"""Are all _1 _2 trip_ids in gtfs sched and events? {all_1_2_in_gtfs_event_sched}""")

    return


@app.cell
def _(mo):
    mo.md(r"""_1_2 in TM events and GTFS events and Schedule, but not in TM schedule for some reason. """)
    return


@app.cell
def _(og):
    og.tm_sched
    return


@app.cell
def _(og, pl):
    og.bus_events.filter(
        pl.col("trip_id").str.contains("OL")
        | pl.col("trip_id").str.contains("_1")
        | pl.col("trip_id").str.contains("_2")
    )["schedule_joined"].unique()
    return


@app.cell
def _(og, pl):
    og.bus_events.filter(
        pl.col("trip_id").str.contains("OL")
        | pl.col("trip_id").str.contains("_1")
        | pl.col("trip_id").str.contains("_2")
    )["schedule_joined"].unique()
    return


@app.cell
def _():
    [
        "trip_id",
        "stop_sequence",
        "tm_stop_sequence",
        "vehicle_label",
        "stop_sequences_vehicle_label_key",
        "trip_id_suffix_removed_lamp_key",
    ]
    return


@app.cell
def _(new):
    new.bus_events.columns
    return


@app.cell
def _(new, og):
    og1 = og.bus_events.with_columns(
        og.bus_events.select(["trip_id", "stop_id", "vehicle_label"]).hash_rows(seed=1337).alias("hash")
    )
    new1 = new.bus_events.with_columns(
        new.bus_events.select(["trip_id_suffix_removed_lamp_key", "stop_id", "vehicle_label"])
        .hash_rows(seed=1337)
        .alias("hash")
    )
    return new1, og1


@app.cell
def _(BusData, pl):
    def filter_bus_data(bus_data: BusData, trip_id_substring):
        new_bus_data = BusData()
        new_bus_data.bus_events = bus_data.bus_events.filter(pl.col("trip_id").str.contains(trip_id_substring))
        new_bus_data.tm_event = bus_data.tm_event.filter(pl.col("trip_id").str.contains(trip_id_substring))
        new_bus_data.gtfs_event = bus_data.gtfs_event.filter(pl.col("trip_id").str.contains(trip_id_substring))
        new_bus_data.tm_sched = bus_data.tm_sched.filter(
            pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).str.contains(trip_id_substring)
        )
        new_bus_data.gtfs_sched = bus_data.gtfs_sched.filter(pl.col("plan_trip_id").str.contains(trip_id_substring))
        new_bus_data.combined_schedule = bus_data.combined_schedule.filter(
            pl.col("trip_id").str.contains(trip_id_substring)
        )
        return new_bus_data

    trip_id_substring = 68162195  # SL3 743 - weird

    return filter_bus_data, trip_id_substring


@app.cell
def _(filter_bus_data, og, trip_id_substring):
    og_68162195 = filter_bus_data(og, trip_id_substring=trip_id_substring)
    og_68162195.bus_events.describe()
    return (og_68162195,)


@app.cell
def _(og_68162195, pl):
    # og_68162195.bus_events.sort('stop_id')
    og_68162195.bus_events.filter(pl.col.vehicle_label == "1318").sort(
        "tm_stop_sequence"
    )  # ('trip_id', 'stop_id', 'vehicle_label').sort('stop_id')
    return


@app.cell
def _(new_68162195):
    new_68162195.bus_events.select(
        "trip_id", "tm_stop_sequence", "stop_id", "vehicle_label", "plan_stop_departure_dt"
    ).sort("vehicle_label")
    return


@app.cell
def _(new_68162195, pl):
    new_68162195.bus_events.with_columns(pl.coalesce("trip_id_suffix_removed_lamp_key"))
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    Schedule does not have OL. So they won't be in combined schedule, meaning they won't get joined in. The thing cory did to get 

    Schedule does have _1 _2. So those will be joined in by this method we have already. 

    GTFS Schedule - gives all pts. also gives _1 _2. missing OL
    TM Schedule - gives all non-rev timepoints. has prefix w/o _1 _2. OL
    """
    )
    return


@app.cell
def _(new, pl):
    new.combined_schedule.filter(pl.col.trip_id == "68245216-OL1")
    return


@app.cell
def _(pl):
    tm_ev = pl.read_parquet("/tmp/tm_events.parquet").filter(pl.col.trip_id == "68245216")
    tm_ev
    return


@app.cell
def _(pl):
    gt_ev = pl.read_parquet("/tmp/gtfs_events.parquet").filter(pl.col.trip_id.str.contains("68245216"))
    gt_ev.sort("trip_id", "vehicle_id", "stop_sequence")
    return


@app.cell
def _(pl):
    gt_sc = pl.read_parquet("/tmp/gtfs_schedule.parquet").filter(pl.col.plan_trip_id.str.contains("68245216"))
    gt_sc
    return


@app.cell
def _(pl):
    df68245216 = pl.read_parquet("68245216.parquet")
    df68245216.sort(by=["vehicle_label", "trip_id_gtfs", "tm_stop_sequence"]).select(
        "trip_id_gtfs",
        "tm_stop_sequence",
        "stop_sequence",
        "timepoint_order",
        "plan_stop_departure_dt",
        "vehicle_label",
        "tm_actual_departure_dt",
    )
    df68245216.sort(by=["vehicle_label", "trip_id_gtfs", "tm_stop_sequence"])
    return


@app.cell
def _():
    # schedule_gtfs.with_columns(pl.coalesce(['trip_id_gtfs', 'trip_id']).alias('trip_id_gtfs')).filter(pl.col.trip_id.str.contains('68245216')).select('trip_id_gtfs', 'vehicle_label', 'vehicle_id').unique()
    # shape: (4, 3)
    # ┌──────────────┬───────────────┬────────────┐
    # │ trip_id_gtfs ┆ vehicle_label ┆ vehicle_id │
    # │ ---          ┆ ---           ┆ ---        │
    # │ str          ┆ str           ┆ str        │
    # ╞══════════════╪═══════════════╪════════════╡
    # │ 68245216-OL1 ┆ 1318          ┆ y1318      │
    # │ 68245216     ┆ 1321          ┆ y1321      │
    # │ 68245216     ┆ null          ┆ null       │
    # │ 68245216     ┆ 1318          ┆ null       │
    # └──────────────┴───────────────┴────────────┘
    # schedule_gtfs.filter(pl.col.trip_id.str.contains('68245216')).select('trip_id_gtfs', 'vehicle_label', 'vehicle_id').unique()
    # shape: (4, 3)
    # ┌──────────────┬───────────────┬────────────┐
    # │ trip_id_gtfs ┆ vehicle_label ┆ vehicle_id │
    # │ ---          ┆ ---           ┆ ---        │
    # │ str          ┆ str           ┆ str        │
    # ╞══════════════╪═══════════════╪════════════╡
    # │ null         ┆ 1318          ┆ null       │
    # │ null         ┆ null          ┆ null       │
    # │ 68245216     ┆ 1321          ┆ y1321      │
    # │ 68245216-OL1 ┆ 1318          ┆ y1318      │
    # └──────────────┴───────────────┴────────────┘
    return


@app.cell
def _(og_68162195):
    og_68162195.gtfs_event.sort("stop_id")
    return


@app.cell
def _(new_68162195):
    new_68162195.gtfs_event.sort("stop_id")
    return


@app.cell
def _():
    return


@app.cell
def _(og_68162195):
    og_68162195
    return


@app.cell
def _(og_68162195):
    og_68162195.bus_events.sort("stop_id")
    return


@app.cell
def _(filter_bus_data, new, trip_id_substring):
    new_68162195 = filter_bus_data(new, trip_id_substring=trip_id_substring)
    new_68162195.bus_events.describe()
    return (new_68162195,)


@app.cell
def _(new_68162195):
    new_68162195.bus_events
    return


@app.cell
def _(new_68162195, plot_lla):
    plot_lla(new_68162195.bus_events, hover="trip_id_gtfs")
    return


@app.cell
def _(new_68162195):
    new_68162195.bus_events  # .select('tm_stop_sequence', "trip_id", "stop_id", "vehicle_label")
    return


@app.cell
def _(new_68162195):
    new_68162195.tm_event
    return


@app.cell
def _(new_68162195):
    new_68162195.combined_schedule.select("tm_stop_sequence", "trip_id", "stop_id")

    # tm_stop_sequence is null in combined schedule, which results in nothing being joined in.
    return


@app.cell
def _(new_68162195, pl):
    new_68162195.tm_sched.filter(pl.col.trip_id.str.contains("68162195"))
    return


@app.cell
def _(new, pl):
    new.tm_sched.filter(pl.col.TRIP_SERIAL_NUMBER == 68162195)
    return


@app.cell
def _(og_68162195):
    og_68162195.bus_events
    return


@app.cell
def _(new_68162195):
    new_68162195.tm_event.sort(by=["vehicle_label", "tm_actual_arrival_dt"])
    return


@app.cell
def _(og):
    og.tm_sched
    return


@app.cell
def _(og, pl):
    og.tm_sched.filter(pl.col("TRIP_SERIAL_NUMBER") == 68245216)
    return


@app.cell
def _(og):
    og.bus_events["trip_id"].str.contains("_1")
    return


@app.cell
def _(new):
    new.bus_events["trip_id"].str.contains("OL")
    return


@app.cell
def _(new1, pl):
    new1.filter((pl.col.trip_id == "69703453") & (pl.col.stop_id == "187")).select("hash")
    return


@app.cell
def _(new1):
    type(new1["hash"])
    return


@app.cell
def _(og1):
    og1.height
    return


@app.cell
def _(new1):
    new1.height
    return


@app.cell
def _(new1, og1, pl):
    pl.concat([og1, new1], how="align").unique(subset="hash")
    return


@app.cell
def _(new):
    new.bus_events
    return


@app.cell
def _(pl):
    df = pl.read_parquet("/tmp/TEST_bus_recent.parquet")
    tm_event = pl.read_parquet("/tmp/tm_events.parquet")
    gtfs_event = pl.read_parquet("/tmp/gtfs_events.parquet")
    tm_sched = pl.read_parquet("/tmp/tm_schedule.parquet")
    gtfs_sched = pl.read_parquet("/tmp/gtfs_schedule.parquet")
    combined_schedule = pl.read_parquet("/tmp/combined_schedule.parquet")
    return combined_schedule, df, gtfs_event, gtfs_sched, tm_event


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(gtfs_event):
    gtfs_event.columns
    return


@app.cell
def _(gtfs_event, pl):
    gtfs_event.filter(pl.col("trip_id_suffix_removed_lamp_key").str.contains("OL"))
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("trip_id").str.contains("OL"))
    return


@app.cell
def _(gtfs_event):
    gtfs_event["trip_id"].unique().sort()
    return


@app.cell
def _(combined_schedule, pl):
    combined_schedule.sort("plan_start_dt", "plan_stop_departure_dt").filter(pl.col.route_id == "104")
    return


@app.cell
def _(df):
    df.describe()
    return


@app.cell
def _(df):
    df["trip_id_suffix_removed_lamp_key"].drop_nulls()
    return


@app.cell
def _():
    return


@app.cell
def _(df, pl):
    df.filter(pl.col.trip_id == "69702986").select("vehicle_label").unique().height == 1
    return


@app.cell
def _(df):
    # works - validate that all trip_ids with OL do not have multiple vehicle_labels
    def validate_single_trip_single_vehicle_OL():
        for id, data in df.group_by("trip_id_suffix_removed_lamp_key"):
            if (data.select("vehicle_label").drop_nulls().unique().height > 1) & (
                data["trip_id_suffix_removed_lamp_key"].str.contains("OL").any()
            ):
                breakpoint()

    validate_single_trip_single_vehicle_OL()
    return


@app.cell
def _(df):
    def validate_single_trip_single_vehicle_1_2():
        for id, data in df.group_by("trip_id_suffix_removed_lamp_key"):
            if (data.select("vehicle_label").drop_nulls().unique().height > 1) & (
                data["trip_id_suffix_removed_lamp_key"].str.contains("_").any()
            ):
                breakpoint()

    validate_single_trip_single_vehicle_1_2()
    return


@app.cell
def _(pl, tm_event):
    tm_event.filter(pl.col("trip_id").str.contains("68662284")).sort("tm_stop_sequence")
    return


@app.cell
def _(gtfs_sched, pl):
    gs = gtfs_sched.filter(pl.col("plan_trip_id").str.contains("_1")).sort("stop_sequence")

    return (gs,)


@app.cell
def _(gs):
    gs
    return


@app.cell
def _(gtfs_event, pl):
    gtfs_event.filter(pl.col("trip_id_gtfs").str.contains("_1") | pl.col("trip_id_gtfs").str.contains("_2")).sort(
        "stop_sequence"
    ).filter(pl.col.trip_id == "68689263")
    return


@app.cell
def _(df, gtfs_event, pl):
    variant_verify = df.filter(
        pl.col.trip_id.is_in(
            gtfs_event.filter(pl.col("trip_id_gtfs").str.contains("_1")).sort("stop_sequence")["trip_id"].unique()
        )
    )
    return (variant_verify,)


@app.cell
def _(variant_verify):
    variant_verify
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _(df, gtfs_event, pl):
    ol_verify = df.filter(
        pl.col.trip_id.is_in(
            gtfs_event.filter(pl.col("trip_id_gtfs").str.contains("-OL")).sort("stop_sequence")["trip_id"].unique()
        )
    )
    return (ol_verify,)


@app.cell
def _(ol_verify, pl):
    ol_verify.filter(pl.col.trip_id == "69898170").sort("vehicle_label", "tm_stop_sequence")
    return


@app.function
def check_all_overload(bus_df):
    for trip_id, data in bus_df.group_by(by="trip_id"):
        print(data.select("trip_id", "trip_id_gtfs"))
        breakpoint()


@app.cell
def _(ol_verify):
    check_all_overload(ol_verify)
    return


@app.cell
def _(date, generate_gtfs_rt_events, gtfs_event, pl):
    from typing import List
    from lamp_py.bus_performance_manager.event_files import event_files_to_load

    def bbb(service_date: date, gtfs_files: List[str], tm_files: List[str]) -> pl.DataFrame:
        gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)
        ol = gtfs_event.filter(pl.col("trip_id").str.contains("OL")).sort("stop_sequence")
        ol = ol.with_columns(pl.col("trip_id").str.replace(r"-OL+", ""))
        print(ol.height)
        if gtfs_event.filter(pl.col("trip_id").is_in(ol["trip_id"].unique().implode())).height > 0:
            breakpoint()
        return gtfs_df

    def scan_dates(start_date, end_date):
        event_files = event_files_to_load(start_date, end_date)

        for service_date in event_files.keys():
            gtfs_files = event_files[service_date]["gtfs_rt"]
            tm_files = event_files[service_date]["transit_master"]

            if len(gtfs_files) == 0:
                continue

            try:
                events_df = bbb(service_date, gtfs_files, tm_files)
            except Exception as e:
                print(e)

    return


@app.cell
def _(datetime, timedelta):
    ed = datetime(year=2025, month=8, day=14)
    sd = ed - timedelta(days=20)
    # scan_dates(start_date=sd, end_date=ed)
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("trip_id").str.contains("68162195"))
    return


if __name__ == "__main__":
    app.run()
