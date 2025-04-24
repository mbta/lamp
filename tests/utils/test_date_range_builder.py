from datetime import datetime
from lamp_py.utils.date_range_builder import build_data_range_paths


def test_simple_case():
    template = "year={yy}/month={mm}/day={dd}/{yy}-{mm:02d}-{dd:02d}T00:00:00.parquet"

    out = build_data_range_paths(template, start_date=datetime(2025, 4, 1), end_date=datetime(2025, 4, 20))
    print(out)

    assert out == [
        "year=2025/month=4/day=1/2025-04-01T00:00:00.parquet",
        "year=2025/month=4/day=2/2025-04-02T00:00:00.parquet",
        "year=2025/month=4/day=3/2025-04-03T00:00:00.parquet",
        "year=2025/month=4/day=4/2025-04-04T00:00:00.parquet",
        "year=2025/month=4/day=5/2025-04-05T00:00:00.parquet",
        "year=2025/month=4/day=6/2025-04-06T00:00:00.parquet",
        "year=2025/month=4/day=7/2025-04-07T00:00:00.parquet",
        "year=2025/month=4/day=8/2025-04-08T00:00:00.parquet",
        "year=2025/month=4/day=9/2025-04-09T00:00:00.parquet",
        "year=2025/month=4/day=10/2025-04-10T00:00:00.parquet",
        "year=2025/month=4/day=11/2025-04-11T00:00:00.parquet",
        "year=2025/month=4/day=12/2025-04-12T00:00:00.parquet",
        "year=2025/month=4/day=13/2025-04-13T00:00:00.parquet",
        "year=2025/month=4/day=14/2025-04-14T00:00:00.parquet",
        "year=2025/month=4/day=15/2025-04-15T00:00:00.parquet",
        "year=2025/month=4/day=16/2025-04-16T00:00:00.parquet",
        "year=2025/month=4/day=17/2025-04-17T00:00:00.parquet",
        "year=2025/month=4/day=18/2025-04-18T00:00:00.parquet",
        "year=2025/month=4/day=19/2025-04-19T00:00:00.parquet",
        "year=2025/month=4/day=20/2025-04-20T00:00:00.parquet",
    ]


def test_year_crossing():
    template = "year={yy}/month={mm}/day={dd}/{yy}-{mm:02d}-{dd:02d}T00:00:00.parquet"

    out = build_data_range_paths(template, start_date=datetime(2024, 12, 30), end_date=datetime(2025, 1, 2))
    print(out)

    assert out == [
        "year=2024/month=12/day=30/2024-12-30T00:00:00.parquet",
        "year=2024/month=12/day=31/2024-12-31T00:00:00.parquet",
        "year=2025/month=1/day=1/2025-01-01T00:00:00.parquet",
        "year=2025/month=1/day=2/2025-01-02T00:00:00.parquet",
    ]


def test_next_leap_year_2028():
    template = "year={yy}/month={mm}/day={dd}/{yy}-{mm:02d}-{dd:02d}T00:00:00.parquet"

    out = build_data_range_paths(template, start_date=datetime(2028, 2, 26), end_date=datetime(2028, 3, 2))
    print(out)

    assert out == [
        "year=2028/month=2/day=26/2028-02-26T00:00:00.parquet",
        "year=2028/month=2/day=27/2028-02-27T00:00:00.parquet",
        "year=2028/month=2/day=28/2028-02-28T00:00:00.parquet",
        "year=2028/month=2/day=29/2028-02-29T00:00:00.parquet",
        "year=2028/month=3/day=1/2028-03-01T00:00:00.parquet",
        "year=2028/month=3/day=2/2028-03-02T00:00:00.parquet",
    ]
