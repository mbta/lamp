import datetime
import json
import polars as pl
from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule


# pylint: disable=R0801
def check_non_null(df: pl.DataFrame, cols: list) -> pl.DataFrame | None:
    """
    verifies columns that should be non-null
    """
    stats = df.describe()
    has_error = False

    for col in cols:
        if not stats.row(by_predicate=pl.col("statistic") == "null_count")[stats.get_column_index(name=col)] in [
            0,
            "0",
            float(0),
        ]:
            df = df.with_columns(pl.col("error_reason").list.concat(pl.lit(f"NON_NULL_{col}")))
            has_error = True

    if has_error:
        return df
    return None


def check_all_unique(df: pl.DataFrame, cols: list) -> pl.DataFrame | None:
    """
    verifies columns that should be all unique
    """
    has_error = False

    for col in cols:
        if not (df[col].drop_nulls().unique_counts() == 1).all():
            df = df.with_columns(pl.col("error_reason").list.concat(pl.lit(f"ALL_UNIQUE_{col}")))
            has_error = True

    if has_error:
        return df
    return None


def check_static_cols(df: pl.DataFrame, cols: list) -> pl.DataFrame | None:
    """
    verifies columns that should have only a single, unique value
    """
    stats = df.describe()
    has_error = False

    for col in cols:
        if not (
            stats.row(by_predicate=pl.col("statistic") == "min")[stats.get_column_index(name=col)]
            == stats.row(by_predicate=pl.col("statistic") == "max")[stats.get_column_index(name=col)]
        ):
            df = df.with_columns(pl.col("error_reason").list.concat(pl.lit(f"STATIC_{col}")))
            has_error = True

    if has_error:
        return df
    return None


DEBUG = False
TMP_DIR = "tmp"
REGENERATE = False
service_date = datetime.date(year=2025, month=6, day=26)

if REGENERATE:
    gtfs_schedule_table = bus_gtfs_schedule_events_for_date(service_date)
    tm_schedule_table = generate_tm_schedule(service_date)
    combined_schedule = pl.DataFrame(join_tm_schedule_to_gtfs_schedule(gtfs_schedule_table, tm_schedule_table))

    gtfs_schedule = pl.DataFrame(gtfs_schedule_table)
    gtfs_schedule.write_parquet(f"gtfs_schedule__service_date_{str(service_date)}.parquet")
    tm_schedule = tm_schedule_table.filter(
        pl.col("TRIP_SERIAL_NUMBER").cast(pl.String).is_in(gtfs_schedule["plan_trip_id"].unique().implode())
    )
    tm_schedule.write_parquet(f"tm_schedule__service_date_{str(service_date)}.parquet")
    combined_schedule.write_parquet(f"combined_schedule__service_date_{str(service_date)}.parquet")

else:
    gtfs_schedule = pl.read_parquet(f"gtfs_schedule__service_date_{str(service_date)}.parquet")
    tm_schedule = pl.read_parquet(f"tm_schedule__service_date_{str(service_date)}.parquet")
    combined_schedule = pl.read_parquet(f"combined_schedule__service_date_{str(service_date)}.parquet")

# 'GEO_NODE_ID', 'GEO_NODE_ABBR', 'stop_id', - can have duplicates because trip stops at stop multiple times
non_null_tm = ["PATTERN_GEO_NODE_SEQ"]
all_unique_tm = [
    "TIME_POINT_ID",
    "TIME_POINT_ABBR",
    "TIME_PT_NAME",
]
static_cols_tm = [
    "TRIP_ID",
    "TRIP_SERIAL_NUMBER",
    "PATTERN_ID",
    "trip_id",
    "tm_planned_sequence_end",
    "tm_planned_sequence_start",
]

SKIP_TM = True
if not SKIP_TM:
    for idx, tm in tm_schedule.group_by("TRIP_ID"):
        check_non_null(tm, non_null_tm)
        check_all_unique(tm, all_unique_tm)
        check_static_cols(tm, static_cols_tm)


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

SKIP_GTFS = True
if not SKIP_GTFS:
    for idx, gtfs in gtfs_schedule.group_by("plan_trip_id"):
        check_non_null(gtfs, non_null_tm)
        check_all_unique(gtfs, all_unique_gtfs)
        check_static_cols(gtfs, static_cols_gtfs)


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
    "schedule_joined",
    "tm_stop_sequence",
    "pattern_id",
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
    "pattern_id",
]

err_dfs = []
skipped_gtfs_trips = set()
combined_schedule = combined_schedule.with_columns(pl.lit([]).alias("error_reason"))
for idx, trip_df in combined_schedule.group_by("trip_id"):
    # how to track this long term? feels wrong here...
    if trip_df["route_id"].drop_nulls().unique().item() == "47":
        continue
        # print(f"trip: {idx} 47 bus detour")

    # contracted - does not show up in TM
    if trip_df["route_id"].drop_nulls().unique().item() == "714":
        continue
    # contracted - does not show up in TM
    if trip_df["route_id"].drop_nulls().unique().item() == "716":
        continue
        # print(f"trip: {idx} 47 bus detour")

    # skip all trip ids with letters
    if not trip_df["trip_id"].cast(pl.UInt32, strict=False).drop_nulls().len() > 0:
        if DEBUG:
            print(f"trip: {idx[0]}")
        skipped_gtfs_trips.add(idx[0])
        continue

    try:
        # for the schedule, make sure no missing rows in any trip
        assert (
            trip_df.filter(pl.col("schedule_joined").is_in(["JOIN", "GTFS"])).height
            == trip_df.select("plan_stop_count").head(1).item()
        )
    except AssertionError as err:
        print(
            f"GTFS has record not present in TM {err} --- trip: {idx} {trip_df.head(1)['trip_id'].item()} joined no TM records"
        )
        err_dfs.append(trip_df.with_columns(pl.col("error_reason").list.concat(pl.lit("GTFS_RECORDS_NOT_IN_TM"))))

    if trip_df["schedule_joined"].is_in(["JOIN", "TM"]).any():
        try:
            assert trip_df.height == trip_df["tm_planned_sequence_end"].drop_nulls().unique().item()
        except AssertionError as err:
            print(
                f"{err} --- trip: {idx} {trip_df.head(1)['trip_id'].item()} Total trip record height does not match expected TM height "
            )
            err_dfs.append(
                trip_df.with_columns(pl.col("error_reason").list.concat(pl.lit("TM_EXPECTED_RECORDS_MISMATCH")))
            )

    else:
        print(
            f"trip: {idx} {trip_df.head(1)['trip_id'].item()} {trip_df.head(1)['service_id'].item()} {trip_df.head(1)['route_id'].item()} joined no TM records"
        )
        err_dfs.append(trip_df.with_columns(pl.col("error_reason").list.concat(pl.lit("NOT_EXPECTED_GTFS_ONLY"))))

    res1 = check_non_null(trip_df, non_null_combined)
    res2 = check_all_unique(trip_df, all_unique_combined)
    res3 = check_static_cols(trip_df, static_cols_combined)

    if res1 is not None:
        err_dfs.append(res1)
    if res2 is not None:
        err_dfs.append(res2)
    if res3 is not None:
        err_dfs.append(res3)

if len(err_dfs) > 0:
    err_df = pl.concat(err_dfs, how="vertical")
    err_df.write_parquet(f"trips_with_issues__service_date_{str(service_date)}.parquet")

with open(f"gtfs_only_trips__service_date_{str(service_date)}.json", encoding="utf-8", mode="w+") as f:
    json.dump(list(skipped_gtfs_trips), f, indent=4)
