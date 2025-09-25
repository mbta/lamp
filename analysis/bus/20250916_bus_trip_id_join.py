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
    from lamp_py.runtime_utils.remote_files import (
        rt_alerts
    )
    from lamp_py.aws.s3 import file_list_from_s3, file_list_from_s3_with_details, object_exists, file_list_from_s3_date_range
    from datetime import datetime, timedelta, date

    return date, datetime, pl, timedelta


@app.cell
def _():
    from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule, CombinedSchedule
    from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events, GTFSEvents
    from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
    from lamp_py.bus_performance_manager.events_tm import generate_tm_events, TransitMasterEvents
    from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
    from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule
    return generate_gtfs_rt_events, join_rt_to_schedule


@app.cell
def _():
    # end_date = datetime(year=2025, month=6, day=12)
    # start_date = end_date - timedelta(days=0)
    prefix = "20250916_bus_trip_id_join__20250612"
    return


@app.cell
def _():
    # run_bus_events(start_date=start_date, end_date=end_date, prefix=prefix)
    return


@app.cell
def _(pl):
    df = pl.read_parquet("/tmp/TEST_bus_recent.parquet")
    tm_event = pl.read_parquet("/tmp/tm_events.parquet")
    gtfs_event = pl.read_parquet("/tmp/gtfs_events.parquet")
    tm_sched = pl.read_parquet("/tmp/tm_schedule.parquet")
    gtfs_sched = pl.read_parquet("/tmp/gtfs_schedule.parquet")
    combined_schedule = pl.read_parquet('/tmp/combined_schedule.parquet')
    return combined_schedule, df, gtfs_event, gtfs_sched, tm_event, tm_sched


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
    gtfs_event['trip_id'].unique().sort()
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
    df['trip_id_suffix_removed_lamp_key'].drop_nulls()
    return


@app.cell
def _():
    
    return


@app.cell
def _(df, pl):
    df.filter(pl.col.trip_id == "69702986").select('vehicle_label').unique().height == 1
    return


@app.cell
def _(df):
    # works - validate that all trip_ids with OL do not have multiple vehicle_labels 
    def validate_single_trip_single_vehicle_OL():
        for id, data in df.group_by("trip_id_suffix_removed_lamp_key"):
            if (data.select('vehicle_label').drop_nulls().unique().height > 1) & (data['trip_id_suffix_removed_lamp_key'].str.contains("OL").any()):
                breakpoint()

    validate_single_trip_single_vehicle_OL()
    return


@app.cell
def _(df):
    def validate_single_trip_single_vehicle_1_2():
        for id, data in df.group_by("trip_id_suffix_removed_lamp_key"):
            if (data.select('vehicle_label').drop_nulls().unique().height > 1) & (data['trip_id_suffix_removed_lamp_key'].str.contains("_").any()):
                breakpoint()


    validate_single_trip_single_vehicle_1_2()
    return


@app.cell
def _(pl, tm_event):
    tm_event.filter(pl.col('trip_id').str.contains('68662284')).sort('tm_stop_sequence')
    return


@app.cell
def _(gtfs_sched, pl):
    gs = gtfs_sched.filter(pl.col('plan_trip_id').str.contains("_1")).sort('stop_sequence')

    return (gs,)


@app.cell
def _(gs):
    gs
    return


@app.cell
def _(gtfs_event, pl):
    gtfs_event.filter(pl.col('trip_id_gtfs').str.contains("_1") | pl.col('trip_id_gtfs').str.contains("_2")).sort('stop_sequence').filter(pl.col.trip_id == "68689263")
    return


@app.cell
def _(df, gtfs_event, pl):
    variant_verify = df.filter(pl.col.trip_id.is_in(gtfs_event.filter(pl.col('trip_id_gtfs').str.contains("_1")).sort('stop_sequence')['trip_id'].unique()))
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
    ol_verify = df.filter(pl.col.trip_id.is_in(gtfs_event.filter(pl.col('trip_id_gtfs').str.contains("-OL")).sort('stop_sequence')['trip_id'].unique()))
    return (ol_verify,)


@app.cell
def _(ol_verify, pl):
    ol_verify.filter(pl.col.trip_id == '69898170').sort('vehicle_label', 'tm_stop_sequence')
    return


@app.function
def check_all_overload(bus_df):
    for trip_id, data in bus_df.group_by(by="trip_id"):
        print(data.select('trip_id', 'trip_id_gtfs'))
        breakpoint()


@app.cell
def _(ol_verify):
    check_all_overload(ol_verify)
    return


@app.cell
def _(bus_df, pl):
    bus_df.filter(pl.col.trip_id == "68689263").select('vehicle_label')
    return


@app.cell
def _(gtfs_event, pl):
    gtfs_event.filter(pl.col.trip_id == "68689263").sort(by="stop_sequence")
    return


@app.cell
def _(pl, tm_event):
    tm_event.filter(pl.col.trip_id.str.contains("68689263"))
    return


@app.cell
def _(pl, tm_sched):
    tm_sched.filter(pl.col.trip_id.str.contains("68689263"))
    return


@app.cell
def _(gtfs_event, pl):
    ol = gtfs_event.filter(pl.col('trip_id').str.contains("OL")).sort('stop_sequence')
    ol = ol.with_columns(pl.col('trip_id').str.replace(r"-OL+", ""))
    return (ol,)


@app.cell
def _(ol):
    ol
    return


@app.cell
def _(gtfs_event, pl):
    unders = gtfs_event.filter(pl.col('trip_id').str.contains(pattern=r"_[1,2]")).sort('stop_sequence')
    unders = unders.with_columns(pl.col('trip_id').alias('trip_id_gtfs'), pl.col('trip_id').str.replace(r"_[1,2]", "")).sort('trip_id', "stop_sequence")
    return (unders,)


@app.cell
def _(unders):
    unders
    return


@app.cell
def _(date, generate_gtfs_rt_events, gtfs_event, pl):
    from typing import List
    from lamp_py.bus_performance_manager.event_files import event_files_to_load
    def bbb(service_date: date, gtfs_files: List[str], tm_files: List[str]) -> pl.DataFrame:
        gtfs_df = generate_gtfs_rt_events(service_date, gtfs_files)
        ol = gtfs_event.filter(pl.col('trip_id').str.contains("OL")).sort('stop_sequence')
        ol = ol.with_columns(pl.col('trip_id').str.replace(r"-OL+", ""))
        print(ol.height)
        if gtfs_event.filter(pl.col('trip_id').is_in(ol['trip_id'].unique().implode())).height > 0:
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
def _(ol):
    ol['trip_id'].unique().implode()
    return


@app.cell
def _(gtfs_event, pl):
    g1 = gtfs_event.with_columns(
        pl.col('trip_id').alias('trip_id_gtfs'), 
        pl.col('trip_id').str.replace(r"-OL+", "")).with_columns(pl.col('trip_id').str.replace(r"_[1,2]", "").alias('trip_id'), )
    g1.filter(pl.col('trip_id').str.contains('68689253')).sort('stop_sequence')
    return (g1,)


@app.cell
def _(g1, pl):
    g1.filter(pl.col('trip_id').str.contains("OL")).is_empty() & g1.filter(pl.col('trip_id').str.contains("_1")).is_empty() & g1.filter(pl.col('trip_id').str.contains("_2")).is_empty()
    return


@app.cell
def _(g1):
    g1
    return


@app.cell
def _(combined_schedule, g1, join_rt_to_schedule, tm_event):
    bus_df = join_rt_to_schedule(combined_schedule, g1, tm_event)
    return (bus_df,)


@app.cell
def _(bus_df):
    bus_df.columns
    return


@app.cell
def _(df, pl):
    df.filter(pl.col('trip_id').str.contains('68162195'))
    return


if __name__ == "__main__":
    app.run()
