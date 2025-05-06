import polars as pl
import pytest


def test_timezone_typing_same_type():
    """
    naive can compare with naive
    aware can compare with aware
    """
    ts = ["2021-03-27 03:00", "2021-03-28 03:00"]
    tz_naive = pl.Series("tz_naive", ts).str.to_datetime()
    assert (tz_naive == tz_naive).rename("naive_compared").all()
    tz_aware = tz_naive.dt.replace_time_zone("UTC").rename("tz_aware")
    assert (tz_aware == tz_aware).rename("naive_compared").all()


def test_timezone_typing_us_eastern_vs_america_new_york_fail():
    """
    US/Eastern can not compare with America/New_York even though they are both EDT/EST
    """
    ts = ["2021-03-27 03:00", "2021-03-28 03:00"]
    tz_naive = pl.Series("tz_naive", ts).str.to_datetime()
    tz_aware_ny = tz_naive.dt.replace_time_zone("America/New_York").rename("tz_aware_ny")
    tz_aware_eastern = tz_naive.dt.replace_time_zone("US/Eastern").rename("tz_aware_east")
    try:
        out_compared2 = (tz_aware_ny > tz_aware_eastern).rename("ny_vs_eastern")
        # this should fail...if it doesn't, something has gone awry
        assert False
    except pl.exceptions.SchemaError:
        assert True


def test_timezone_typing_us_eastern_vs_utc_fail():
    """
    UTC can not compare with America/New_York
    """
    ts = ["2021-03-27 03:00", "2021-03-28 03:00"]
    tz_naive = pl.Series("tz_naive", ts).str.to_datetime()
    tz_aware = tz_naive.dt.replace_time_zone("UTC").rename("tz_aware")
    tz_aware_ny = tz_naive.dt.replace_time_zone("America/New_York").rename("tz_aware_ny")
    try:
        out_compared = (tz_aware_ny > tz_aware).rename("out compared")
        # this should fail...if it doesn't, something has gone awry
        assert False
    except pl.exceptions.SchemaError:
        assert True


def test_timezone_typing_tz_vs_naive_fail():
    """
    verify can't compare naive with aware
    """
    ts = ["2021-03-27 03:00", "2021-03-28 03:00"]
    tz_naive = pl.Series("tz_naive", ts).str.to_datetime()
    tz_aware = tz_naive.dt.replace_time_zone("UTC").rename("tz_aware")

    try:
        # can't compare naive with aware
        out_compared = tz_naive > tz_aware
        # this should fail...if it doesn't, something has gone awry
        assert False
    except pl.exceptions.SchemaError:
        assert True
