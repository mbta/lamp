    

import datetime
import polars as pl
from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule

REGENERATE = False
service_date = datetime.date(year=2025, month=8, day=12)

if REGENERATE:
    gtfs_schedule = bus_gtfs_schedule_events_for_date(service_date)
    tm_schedule = generate_tm_schedule()
    combined_schedule = join_tm_schedule_to_gtfs_schedule(gtfs_schedule, tm_schedule)

    gtfs_schedule.write_parquet("gtfs_schedule.parquet")
    tm_schedule.tm_schedule.collect().write_parquet("tm_schedule.parquet")
    combined_schedule.write_parquet("combined_schedule.parquet")

else:
    gtfs_schedule = pl.read_parquet('gtfs_schedule.parquet')
    tm_schedule = pl.read_parquet('tm_schedule.parquet').filter(pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs_schedule["plan_trip_id"].unique()))
    combined_schedule = pl.read_parquet('combined_schedule.parquet')




tm_schedule = tm_schedule.filter(pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs_schedule["plan_trip_id"].unique()))
# check gtfs_schedule

def check_non_null(df, cols):
    stats = df.describe()

    for col in cols:
        assert(stats.row(by_predicate=pl.col('statistic') == "null_count")[stats.get_column_index(name=col)] == 0)

def check_all_unique(df, static_cols):
    for col in static_cols:
        assert((df[col].drop_nulls().unique_counts() == 1).all())

def check_static_cols(df, static_cols):
    stats = df.describe()

    for col in static_cols:
        assert(stats.row(by_predicate=pl.col('statistic') == "min")[stats.get_column_index(name=col)] == stats.row(by_predicate=pl.col('statistic') == "max")[stats.get_column_index(name=col)])

non_null_tm = ["PATTERN_GEO_NODE_SEQ"]
all_unique_tm = [ 'TIME_POINT_ID', 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'TIME_POINT_ABBR', 'TIME_PT_NAME', 'stop_id', ]
static_cols_tm = ['TRIP_ID', 'TRIP_SERIAL_NUMBER', 'PATTERN_ID','trip_id', 'tm_planned_sequence_end', 'tm_planned_sequence_start']
for idx, tm in tm_schedule.group_by("TRIP_ID"):
    check_non_null(tm, non_null_tm)
    check_all_unique(tm, all_unique_tm)
    check_static_cols(tm, static_cols_tm)
    

  
# incrementing = ['PATTERN_GEO_NODE_SEQ', 'TIME_POINT_ID', 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'TIME_POINT_ABBR', 'TIME_PT_NAME', 'stop_id', ]
non_null_tm = [
  "stop_sequence",
]
all_unique_gtfs = [
  "stop_id",
  "stop_sequence",
  "stop_name",
]
static_cols_gtfs = [
  "plan_trip_id",
  "block_id",
  "route_id",
  "service_id",
  "route_pattern_id",
  "route_pattern_typicality",
  "direction_id",
  "direction",
  "direction_destination",
  "plan_stop_count",
  "plan_start_time",
  "plan_start_dt",
]

for idx, gtfs in gtfs_schedule.group_by("planned_trip_id"):
    check_non_null(gtfs, non_null_tm)
    check_all_unique(gtfs, all_unique_gtfs)
    check_static_cols(gtfs, static_cols_gtfs)



# incrementing = ['PATTERN_GEO_NODE_SEQ', 'TIME_POINT_ID', 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'TIME_POINT_ABBR', 'TIME_PT_NAME', 'stop_id', ]
static_cols_combined = [
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
  "pattern_id"
]
for idx, combined in combined_schedule.group_by("trip_id"):
    # check_non_null(combined, non_null_combined)
    # check_all_unique(combined, all_unique_combined)
    check_static_cols(combined, static_cols_combined)