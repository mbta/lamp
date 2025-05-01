# type: ignore
import datetime
import polars as pl
from pyarrow import dataset as pd

# ds = pd.dataset("s3://mbta-ctd-dataplatform-staging-archive/lamp/tableau/rail/LAMP_ALL_RT_fields.parquet")
ds = pd.dataset("https://performancedata.mbta.com/lamp/tableau/rail/LAMP_ALL_RT_fields.parquet")
dates = []
# todo - 30 days? 31 days?
for day in range(1, 31):
    date = datetime.datetime(2025, 4, day)
    for bat in ds.to_batches(
        batch_size=500_000, batch_readahead=5, fragment_readahead=0, columns=["service_date", "route_id"]
    ):
        # breakpoint()
        pls = pl.from_arrow(bat)
        res = pls.filter(pl.col("service_date") == date)
        # breakpoint()
        if res.height > 0:
            dates.append(date)
            print(f"ok: {date}, {res.height}")
        # print(".")÷

assert all([(dates[i + 1] - dates[i]).days < 2 for i in range(len(dates) - 1)])
