    

import datetime
import polars as pl
from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule

def check_non_null(df, cols, trip_id="", prefix="") -> pl.DataFrame | None:
    stats = df.describe()

    for col in cols:
        if stats.row(by_predicate=pl.col('statistic') == "null_count")[stats.get_column_index(name=col)] in [0, "0", float(0)]:
            continue
        else:
            return df
            df.write_parquet(f'{tmp_dir}/{prefix}_{trip_id}__fail_non_null_{col}.parquet')

def check_all_unique(df, static_cols, trip_id="", prefix="") -> pl.DataFrame | None:
    for col in static_cols:
        if (df[col].drop_nulls().unique_counts() == 1).all():
            continue
        else:
            return df
            # df[col].drop_nulls().value_counts().filter(pl.col.count == 2).select('stop_name').item().replace(' ', '-')
            df.write_parquet(f'{tmp_dir}/{prefix}_{trip_id}__fail_unique_{col}.parquet')

def check_static_cols(df, static_cols, trip_id="", prefix="") -> pl.DataFrame | None:
    stats = df.describe()

    for col in static_cols:
        if stats.row(by_predicate=pl.col('statistic') == "min")[stats.get_column_index(name=col)] == stats.row(by_predicate=pl.col('statistic') == "max")[stats.get_column_index(name=col)]:
            continue
        else:
            return df
            # df.write_parquet(f'{tmp_dir}/{prefix}_{trip_id}__fail_static_{col}.parquet')


tmp_dir = "tmp"
REGENERATE = False
service_date = datetime.date(year=2025, month=8, day=12)

if REGENERATE:
    gtfs_schedule = bus_gtfs_schedule_events_for_date(service_date)
    tm_schedule = generate_tm_schedule()
    combined_schedule = join_tm_schedule_to_gtfs_schedule(gtfs_schedule, tm_schedule)

    gtfs_schedule.write_parquet("gtfs_schedule.parquet")
    tm_schedule.tm_schedule.collect().write_parquet("tm_schedule.parquet")
    tm_schedule = tm_schedule.tm_schedule.collect()
    combined_schedule.write_parquet("combined_schedule.parquet")

else:
    gtfs_schedule = pl.read_parquet('gtfs_schedule.parquet')
    tm_schedule = pl.read_parquet('tm_schedule.parquet')
    combined_schedule = pl.read_parquet('combined_schedule.parquet')

tm_schedule = tm_schedule.filter(pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs_schedule["plan_trip_id"].unique().implode()))
# check gtfs_schedule


# 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'stop_id', - can have duplicates because trip stops at stop multiple times
non_null_tm = ["PATTERN_GEO_NODE_SEQ"]
all_unique_tm = [ 'TIME_POINT_ID',  'TIME_POINT_ABBR', 'TIME_PT_NAME',  ]
static_cols_tm = ['TRIP_ID', 'TRIP_SERIAL_NUMBER', 'PATTERN_ID','trip_id', 'tm_planned_sequence_end', 'tm_planned_sequence_start']

skip_tm = True
if not skip_tm:
    for idx, tm in tm_schedule.group_by("TRIP_ID"):
        check_non_null(tm, non_null_tm, trip_id=idx[0], prefix="tm")
        check_all_unique(tm, all_unique_tm, trip_id=idx[0], prefix="tm")
        check_static_cols(tm, static_cols_tm, trip_id=idx[0], prefix="tm")
    

  
# incrementing = ['PATTERN_GEO_NODE_SEQ', 'TIME_POINT_ID', 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'TIME_POINT_ABBR', 'TIME_PT_NAME', 'stop_id', ]
non_null_tm = [
  "stop_sequence",
]
all_unique_gtfs = [
#   "stop_id",
  "stop_sequence",
#   "stop_name", # multiple stops can have the same stop name...but different stop Ids
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

skip_gtfs = True
if not skip_gtfs:
    for idx, gtfs in gtfs_schedule.group_by("plan_trip_id"):
        check_non_null(gtfs, non_null_tm, trip_id=idx[0], prefix="gtfs")
        check_all_unique(gtfs, all_unique_gtfs, trip_id=idx[0], prefix="gtfs")
        check_static_cols(gtfs, static_cols_gtfs, trip_id=idx[0], prefix="gtfs")



# incrementing = ['PATTERN_GEO_NODE_SEQ', 'TIME_POINT_ID', 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'TIME_POINT_ABBR', 'TIME_PT_NAME', 'stop_id', ]
non_null_combined = [
  "trip_id",
  "block_id",
  "route_id",
  "service_id",
  "route_pattern_id",
  "route_pattern_typicality",
  "direction_id",
  "direction",
  "direction_destination",
  "tm_joined",
  "tm_stop_sequence",
  "pattern_id"
]

all_unique_combined = [
  "stop_sequence",
#   "stop_name",
#   "stop_id",

]

static_cols_combined = [
  "trip_id",
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
  "tm_planned_sequence_start",
  "tm_planned_sequence_end",
  "pattern_id"
]

err_combined = pl.DataFrame(schema=combined_schedule.schema)
df_list = []

# dataframe of all suspect trips

for idx, combined in combined_schedule.group_by("trip_id"):

    res1 = check_non_null(combined, non_null_combined, trip_id=idx[0], prefix="combined")
    res2 = check_all_unique(combined, all_unique_combined, trip_id=idx[0], prefix="combined")
    res3 = check_static_cols(combined, static_cols_combined, trip_id=idx[0], prefix="combined")

    if combined.filter(pl.col("trip_id").str.contains("Blue")):
        # add something to a dataframe...
        pass

    
    try:
        # for the schedule, make sure no missing rows in any trip
        assert combined.filter(pl.col("tm_joined").is_in(["JOIN", "GTFS"])).height == combined.select("plan_stop_count").head(1).item()
    except AssertionError as err:
        print(f"GTFS has record not present in TM {err} --- trip: {idx} {combined.head(1)['trip_id'].item()} joined no TM records")
    
    if (combined["tm_joined"].is_in(["JOIN", "TM"]).any()):
        try:
            assert combined.height == combined['tm_planned_sequence_end'].drop_nulls().unique().item()
        except AssertionError as err:
            print(f"{err} --- trip: {idx} {combined.head(1)['trip_id'].item()} Total trip record height does not match expected TM height ")

    else:
        print(f"trip: {idx} {combined.head(1)['trip_id'].item()} joined no TM records")
    # breakpoint()