from lamp_py.ingestion.utils import hash_gtfs_rt_table
import polars as pl
import io
from typing import Any
import hashlib
import pickle


def hash_gtfs_rt_row_marked_for_removal_wip(row: Any) -> bytes:
    """hash row from polars dataframe"""
    return hashlib.md5(pickle.dumps(row), usedforsecurity=False).digest()


def test_hash_column_return_datatype() -> None:
    """
    return type of hash_gtfs_rt_row of Tuple[bytes] was coerced to binary before, but polars changes in 1.35 -> 1.36
    resulted in the type now returning List[Binary]. This test is to catch changes in underlying inferred types
    for future library upgrades.

    We are no longer coercing the type, so this might never catch.

    Remove this test if we ever remove the hash_gtfs_rt_row type logic from ingestion.

    Investigate using just the base polars hash instead...why no

    Check that all are hashable. check that same columns hash to the same value
    """

    table = pl.DataFrame(
        [
            pl.Series("month", [10, 10, 11], dtype=pl.Int64),
            pl.Series("year", [2025, 2025, 2025], dtype=pl.Int64),
            pl.Series("id", ["71194552", "71194552", "71194553"], dtype=pl.String),
            pl.Series("day", [24, 24, 25], dtype=pl.Int64),
            pl.Series("feed_timestamp", [1761264002, 1761264023, 1761264003], dtype=pl.Int64),
        ]
    )

    # the return type should be pl.Binary - ensure this remains valid
    table_hash = table.with_columns(
        table.select(["year", "month", "day", "id"])
        .map_rows(hash_gtfs_rt_row_marked_for_removal_wip, return_dtype=pl.Binary)
        .to_series()
        .alias("hash_column")
    )

    # check that the hash values for the equal rows (excluding feed_timestamp)  are equal
    assert len(table_hash.filter(pl.col("id") == "71194552")["hash_column"].unique()) == 1
    assert table_hash["hash_column"].null_count() == 0

    # test hash_rows() - internally consistent only within single version of polars
    # but this hash table function should only be used in memory...don't store this and compare
    table_hash2 = table.with_columns(table.select(["year", "month", "day", "id"]).hash_rows().alias("hash_column"))

    # check that the hash values for the equal rows (excluding feed_timestamp)  are equal
    assert len(table_hash2.filter(pl.col("id") == "71194552")["hash_column"].unique()) == 1
    assert table_hash2["hash_column"].null_count() == 0


def test_polars_hashrows_works_for_all_trip_update_col_types() -> None:
    """
    Test hash_rows() works on a sample row of real data from trip_updates

    Check that hash_row() and the custom hash_gtfs_rt_row() return the same sets when their individual hashes are applied
    """

    table = pl.read_json(
        io.StringIO(
            '[{"id":"71194552","trip_update.trip.trip_id":"71194552","trip_update.trip.route_id":"Green-E","trip_update.trip.direction_id":1,"trip_update.trip.start_time":"20:30:00","trip_update.trip.start_date":"20251023","trip_update.trip.schedule_relationship":null,"trip_update.trip.route_pattern_id":"Green-E-886-1","trip_update.trip.tm_trip_id":null,"trip_update.trip.overload_id":null,"trip_update.trip.overload_offset":null,"trip_update.trip.revenue":true,"trip_update.trip.last_trip":false,"trip_update.vehicle.id":"G-10052","trip_update.vehicle.label":null,"trip_update.vehicle.license_plate":null,"trip_update.vehicle.consist":null,"trip_update.vehicle.assignment_status":null,"trip_update.timestamp":1761263939,"trip_update.delay":null,"year":2025,"month":10,"day":24,"feed_timestamp":1761264002,"trip_update.stop_time_update.stop_sequence":1,"trip_update.stop_time_update.stop_id":"70260","trip_update.stop_time_update.arrival.delay":null,"trip_update.stop_time_update.arrival.time":null,"trip_update.stop_time_update.arrival.uncertainty":null,"trip_update.stop_time_update.departure.delay":null,"trip_update.stop_time_update.departure.time":1761265800,"trip_update.stop_time_update.departure.uncertainty":360,"trip_update.stop_time_update.schedule_relationship":null,"trip_update.stop_time_update.boarding_status":null}]'
        )
    )
    table2 = pl.read_json(
        io.StringIO(
            '[{"id":"71194552","trip_update.trip.trip_id":"71194552","trip_update.trip.route_id":"Green-E","trip_update.trip.direction_id":1,"trip_update.trip.start_time":"20:30:00","trip_update.trip.start_date":"20251023","trip_update.trip.schedule_relationship":null,"trip_update.trip.route_pattern_id":"Green-E-886-1","trip_update.trip.tm_trip_id":null,"trip_update.trip.overload_id":null,"trip_update.trip.overload_offset":null,"trip_update.trip.revenue":true,"trip_update.trip.last_trip":false,"trip_update.vehicle.id":"G-10052","trip_update.vehicle.label":null,"trip_update.vehicle.license_plate":null,"trip_update.vehicle.consist":null,"trip_update.vehicle.assignment_status":null,"trip_update.timestamp":1761263939,"trip_update.delay":null,"year":2025,"month":10,"day":24,"feed_timestamp":1761264002,"trip_update.stop_time_update.stop_sequence":1,"trip_update.stop_time_update.stop_id":"70260","trip_update.stop_time_update.arrival.delay":null,"trip_update.stop_time_update.arrival.time":null,"trip_update.stop_time_update.arrival.uncertainty":null,"trip_update.stop_time_update.departure.delay":null,"trip_update.stop_time_update.departure.time":1761265800,"trip_update.stop_time_update.departure.uncertainty":360,"trip_update.stop_time_update.schedule_relationship":null,"trip_update.stop_time_update.boarding_status":null}]'
        )
    )
    table3 = pl.read_json(
        io.StringIO(
            '[{"id":"71194553","trip_update.trip.trip_id":"71194552","trip_update.trip.route_id":"Green-E","trip_update.trip.direction_id":1,"trip_update.trip.start_time":"20:30:00","trip_update.trip.start_date":"20251023","trip_update.trip.schedule_relationship":null,"trip_update.trip.route_pattern_id":"Green-E-886-1","trip_update.trip.tm_trip_id":null,"trip_update.trip.overload_id":null,"trip_update.trip.overload_offset":null,"trip_update.trip.revenue":true,"trip_update.trip.last_trip":false,"trip_update.vehicle.id":"G-10052","trip_update.vehicle.label":null,"trip_update.vehicle.license_plate":null,"trip_update.vehicle.consist":null,"trip_update.vehicle.assignment_status":null,"trip_update.timestamp":1761263939,"trip_update.delay":null,"year":2025,"month":10,"day":24,"feed_timestamp":1761264002,"trip_update.stop_time_update.stop_sequence":1,"trip_update.stop_time_update.stop_id":"70260","trip_update.stop_time_update.arrival.delay":null,"trip_update.stop_time_update.arrival.time":null,"trip_update.stop_time_update.arrival.uncertainty":null,"trip_update.stop_time_update.departure.delay":null,"trip_update.stop_time_update.departure.time":1761265800,"trip_update.stop_time_update.departure.uncertainty":360,"trip_update.stop_time_update.schedule_relationship":null,"trip_update.stop_time_update.boarding_status":null}]'
        )
    )

    table4 = pl.concat([table, table2, table3])
    table_hash3 = table4.with_columns(
        table4.drop("feed_timestamp").hash_rows().alias("hash_column"),
        table4.drop("feed_timestamp")
        .map_rows(hash_gtfs_rt_row_marked_for_removal_wip, return_dtype=pl.Binary)
        .to_series()
        .alias("hash_column_og"),
    )

    # all rows get hashed
    assert table_hash3["hash_column"].null_count() == 0
    assert table_hash3["hash_column_og"].null_count() == 0

    # both hash functions return the same set - so they are consistent, as expected
    assert table_hash3.select("hash_column", "hash_column_og").unique().height == 2
