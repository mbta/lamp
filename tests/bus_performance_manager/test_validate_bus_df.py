import os
import polars as pl
import pyarrow.parquet as pq

# input data from here - regenerate by running bus_performance_metrics() and removing the drops
# analysis/check_bus.py has a bus_performance_metrics() runner

# bus = pq.read_table('bus_df_final_validation_frames_not_dropped.parquet')
# bus = pl.from_arrow(bus)
# dumps the first 20 trip_ids to process
# for name, data in bus.group_by(["trip_id"]):
# print(name)
# data.write_csv(f"bus_test_case_{idx}_trip_id_{name[0]}.csv")
# idx += 1
# if idx > 20:
#     break


def test_bus_performance_metrics_times():
    """
    Validate that when there are valid inputs to the stop_arrival_dt or stop_departure_dt, then
    these two fields are properly filled out. validate the last stop time is populated

    only checks the last stop at the moment - TODO expand to test all rows
    """
    base_dir = "tests/test_files/bus_performance_manager/validate_bus_df/"
    files = os.listdir(base_dir)
    for f in files:
        data = pl.read_csv(os.path.join(base_dir, f))
        print(data["trip_id"].unique())

        test = data.select(["tm_actual_arrival_dt", "gtfs_travel_to_dt", "gtfs_arrival_dt", "stop_arrival_dt"]).tail(1)
        if test["stop_arrival_dt"].is_null()[0]:
            print(test)
            # check both inputs are actually null
            if not (test["tm_actual_arrival_dt"].is_null()[0] & test["gtfs_arrival_dt"].is_null()[0]):
                assert False
                # breakpoint()
            print(f"{f} - valid null - no inputs arrival")

        # full = data.select([ 'tm_actual_arrival_dt', 'gtfs_travel_to_dt', 'gtfs_arrival_dt', 'stop_arrival_dt'])
        # breakpoint()
        # full.with_columns(
        # pl.when(full['stop_arrival_dt'].is_null() & full['tm_actual_arrival_dt'].is_null())
        # .then(pl.lit(False))
        #     print(full)
        #     # check both inputs are actually null
        #     if not (full['tm_actual_arrival_dt'].is_null()[0] & full['gtfs_arrival_dt'].is_null()[0]):
        #         assert False
        #         # breakpoint()
        #     print(f'{f} - valid null - no inputs arrival')

        test2 = data.select(
            ["tm_actual_departure_dt", "stop_arrival_dt", "gtfs_departure_dt", "stop_departure_dt"]
        ).tail(1)

        if test2["stop_departure_dt"].is_null()[0]:
            print(test2)
            # check both inputs are actually null
            assert test2["tm_actual_departure_dt"].is_null()[0] & test2["gtfs_departure_dt"].is_null()[0]
            print(f"{f} - valid null - no inputs departure")

    assert True
