# What would a response from a `recent_events` endpoint look like?
crunkel@mbta.com
2025-10-31

I want to create a data sample to align with GO and the API codeowners
on the interface for Flashback. I could do this in Notion but I think
that it would be helpful to try it in our codebase with real data to
think through some of the mechanics.

For this sample, I’ll use the aggregated responses of the enhanced
Vehicle Positions endpoint:

<details open class="code-fold">
<summary>Code</summary>

``` python
import polars as pl

uri = "s3://mbta-ctd-dataplatform-springboard/lamp/RT_VEHICLE_POSITIONS/year=2025/month=11/day=4/2025-11-04T00:00:00.parquet"

(
    pl.scan_parquet(uri)
    .head(5)
    .collect()
)
```

</details>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 32)</small>

| id | vehicle.trip.trip_id | vehicle.trip.route_id | vehicle.trip.direction_id | vehicle.trip.start_time | vehicle.trip.start_date | vehicle.trip.schedule_relationship | vehicle.trip.route_pattern_id | vehicle.trip.tm_trip_id | vehicle.trip.overload_id | vehicle.trip.overload_offset | vehicle.trip.revenue | vehicle.trip.last_trip | vehicle.vehicle.id | vehicle.vehicle.label | vehicle.vehicle.license_plate | vehicle.vehicle.consist | vehicle.vehicle.assignment_status | vehicle.position.bearing | vehicle.position.latitude | vehicle.position.longitude | vehicle.position.speed | vehicle.position.odometer | vehicle.current_stop_sequence | vehicle.stop_id | vehicle.current_status | vehicle.timestamp | vehicle.congestion_level | vehicle.occupancy_status | vehicle.occupancy_percentage | vehicle.multi_carriage_details | feed_timestamp |
|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| str | str | str | u8 | str | str | str | str | str | i64 | i64 | bool | bool | str | str | str | list\[struct\[1\]\] | str | u16 | f64 | f64 | f64 | f64 | u32 | str | str | u64 | str | str | u32 | list\[struct\[5\]\] | u64 |
| "G-10231" | "71193828" | "Green-C" | 1 | "18:39:00" | "20251103" | "SCHEDULED" | null | null | null | null | true | false | "G-10231" | "3611-3890" | null | null | null | 45 | 42.34546 | -71.10899 | 5.2 | null | 300 | "70212" | "INCOMING_AT" | 1762214425 | null | null | null | \[{null,"3611","NO_DATA_AVAILABLE",null,1}, {null,"3890","NO_DATA_AVAILABLE",null,2}\] | 1762214429 |
| "G-10034" | "71193825" | "Green-C" | 1 | "18:56:00" | "20251103" | "SCHEDULED" | null | null | null | null | true | false | "G-10034" | "3699-3859" | null | null | null | 45 | 42.3392 | -71.13568 | 4.5 | null | 220 | "70230" | "INCOMING_AT" | 1762214462 | null | null | null | \[{null,"3699","NO_DATA_AVAILABLE",null,1}, {null,"3859","NO_DATA_AVAILABLE",null,2}\] | 1762214466 |
| "G-10034" | "71193825" | "Green-C" | 1 | "18:56:00" | "20251103" | "SCHEDULED" | null | null | null | null | true | false | "G-10034" | "3699-3859" | null | null | null | 45 | 42.33865 | -71.13785 | 6.2 | null | 220 | "70230" | "INCOMING_AT" | 1762214426 | null | null | null | \[{null,"3699","NO_DATA_AVAILABLE",null,1}, {null,"3859","NO_DATA_AVAILABLE",null,2}\] | 1762214432 |
| "G-10231" | "71193828" | "Green-C" | 1 | "18:39:00" | "20251103" | "SCHEDULED" | null | null | null | null | true | false | "G-10231" | "3611-3890" | null | null | null | 45 | 42.34571 | -71.10809 | 5.0 | null | 300 | "70212" | "INCOMING_AT" | 1762214440 | null | null | null | \[{null,"3611","NO_DATA_AVAILABLE",null,1}, {null,"3890","NO_DATA_AVAILABLE",null,2}\] | 1762214443 |
| "G-10103" | "71193813" | "Green-C" | 0 | "18:38:00" | "20251103" | "SCHEDULED" | null | null | null | null | true | false | "G-10103" | "3886-3696" | null | null | null | 225 | 42.34171 | -71.12314 | 4.9 | null | 370 | "70223" | "INCOMING_AT" | 1762214395 | null | null | null | \[{null,"3886","NO_DATA_AVAILABLE",null,1}, {null,"3696","NO_DATA_AVAILABLE",null,2}\] | 1762214400 |

</div>

As part of Bus Performance Manager, LAMP created logic to transform
these responses into events.

<details open class="code-fold">
<summary>Code</summary>

``` python
from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events
```

</details>

The function takes as an input an s3 URI so we’ll pass the same URI
again:

<details open class="code-fold">
<summary>Code</summary>

``` python
from datetime import date

sample = generate_gtfs_rt_events(date(2025, 11, 4), [uri])
```

</details>

    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=7bd49d69-8d97-4fc2-83dd-6a125ffcabd0, process_id=34002, status=started, free_disk_mb=339759, free_mem_pct=25, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=7c29ce9c-6d63-44b8-94a3-4e232378282d, process_id=34002, status=started, free_disk_mb=339759, free_mem_pct=25, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=224602da-e0b4-4ad1-8512-b1fb1cf75067, process_id=34002, status=started, free_disk_mb=339759, free_mem_pct=25, file=routes, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=224602da-e0b4-4ad1-8512-b1fb1cf75067, process_id=34002, status=add_metadata, free_disk_mb=339759, free_mem_pct=25, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=224602da-e0b4-4ad1-8512-b1fb1cf75067, process_id=34002, status=add_metadata, free_disk_mb=339759, free_mem_pct=25, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=224602da-e0b4-4ad1-8512-b1fb1cf75067, process_id=34002, status=complete, free_disk_mb=339759, free_mem_pct=25, duration=0.90, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=7c29ce9c-6d63-44b8-94a3-4e232378282d, process_id=34002, status=complete, free_disk_mb=339757, free_mem_pct=25, duration=3.12, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=7bd49d69-8d97-4fc2-83dd-6a125ffcabd0, process_id=34002, status=add_metadata, free_disk_mb=339757, free_mem_pct=25, service_date=2025-11-04, rows_from_parquet=3563271
    INFO:root:parent=unknown, process_name=position_to_events, uuid=022b8f9d-d170-46ca-beaa-a470acbd8775, process_id=34002, status=started, free_disk_mb=339757, free_mem_pct=24, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=022b8f9d-d170-46ca-beaa-a470acbd8775, process_id=34002, status=add_metadata, free_disk_mb=339757, free_mem_pct=24, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=022b8f9d-d170-46ca-beaa-a470acbd8775, process_id=34002, status=complete, free_disk_mb=339757, free_mem_pct=24, duration=0.00, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=7bd49d69-8d97-4fc2-83dd-6a125ffcabd0, process_id=34002, status=add_metadata, free_disk_mb=339757, free_mem_pct=24, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=7bd49d69-8d97-4fc2-83dd-6a125ffcabd0, process_id=34002, status=complete, free_disk_mb=339757, free_mem_pct=24, duration=4.02, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038

A close reading of the log message shows that this function takes 6
seconds to run for a whole day. This is neither terrible nor great for a
whole day and I think it could be sped up by removing the `.collect()`
statement in the middle of this code and remove some logging that’s not
needed for flashback.

Now to the real question: what does this sample look like?

<details open class="code-fold">
<summary>Code</summary>

``` python
sample.head(5)
```

</details>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 17)</small>

| trip_id | stop_id | route_id | service_date | start_time | start_dt | gtfs_stop_sequence | stop_count | direction_id | vehicle_id | vehicle_label | gtfs_first_in_transit_dt | gtfs_last_in_transit_dt | gtfs_arrival_dt | gtfs_departure_dt | latitude | longitude |
|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| str | str | str | date | i64 | datetime\[μs\] | i64 | u32 | i8 | str | str | datetime\[μs, UTC\] | datetime\[μs, UTC\] | datetime\[μs, UTC\] | datetime\[μs, UTC\] | f64 | f64 |
| "71694475" | "2681" | "88" | 2025-11-04 | 46200 | 2025-11-04 12:50:00 | 16 | 27 | 1 | "y2080" | "2080" | 2025-11-04 18:03:38 UTC | 2025-11-04 18:03:47 UTC | 2025-11-04 18:03:51 UTC | 2025-11-04 18:04:14 UTC | 42.3894 | -71.106371 |
| "71987666" | "3896" | "230" | 2025-11-04 | 57600 | 2025-11-04 16:00:00 | 48 | 68 | 0 | "y0757" | "0757" | 2025-11-04 21:47:47 UTC | 2025-11-04 21:49:01 UTC | 2025-11-04 21:49:01 UTC | 2025-11-04 21:49:36 UTC | 42.159995 | -71.006223 |
| "71466060" | "1475" | "17" | 2025-11-04 | 57000 | 2025-11-04 15:50:00 | 16 | 28 | 1 | "y1904" | "1904" | 2025-11-04 21:01:11 UTC | 2025-11-04 21:01:17 UTC | null | null | 42.311536 | -71.0631 |
| "71465731" | "34" | "7" | 2025-11-04 | 54480 | 2025-11-04 15:08:00 | 3 | 15 | 1 | "y1793" | "1793" | 2025-11-04 20:09:45 UTC | 2025-11-04 20:09:59 UTC | 2025-11-04 20:10:02 UTC | 2025-11-04 20:10:12 UTC | 42.33825 | -71.027297 |
| "71466081" | "21148" | "45" | 2025-11-04 | 42900 | 2025-11-04 11:55:00 | 25 | 26 | 1 | "y1818" | "1818" | 2025-11-04 17:15:01 UTC | 2025-11-04 17:15:07 UTC | 2025-11-04 17:15:07 UTC | 2025-11-04 17:15:26 UTC | 42.331237 | -71.092055 |

</div>

Parity with the `predictions` endpoint requires filtering by:

- `latitude`
- `longitude`
- `direction_id`
- `route_type`
- `stop`
- `route`
- `trip`
- `revenue`
- `route_pattern`

So, this sample doesn’t give exactly the fields needed but it has the
real-time components that users won’t get anywhere else:

- `vehicle_id`
- `gtfs_arrival_dt`
- `gtfs_departure_dt`

I want to replicate the structure of [realtime trip
updates](https://cdn.mbta.com/realtime/TripUpdates_enhanced.json) here:

``` json
[
    {
      "id": "72040316",
      "trip_update": {
        "timestamp": 1762442485,
        "stop_time_update": [
          {
            "stop_id": "74614",
            "stop_sequence": 1,
            "departure": {
              "time": 1762444800,
              "uncertainty": 300
            }
          },
          {
            "stop_id": "74615",
            "stop_sequence": 2,
            "arrival": {
              "time": 1762444948,
              "uncertainty": 300
            },
            "departure": {
              "time": 1762444948,
              "uncertainty": 300
            }
          },
          {
            "stop_id": "74616",
            "stop_sequence": 3,
            "arrival": {
              "time": 1762445026,
              "uncertainty": 300
            },
            "departure": {
              "time": 1762445026,
              "uncertainty": 300
            }
          },
          {
            "stop_id": "74617",
            "stop_sequence": 4,
            "arrival": {
              "time": 1762445144,
              "uncertainty": 300
            }
          }
        ],
        "trip": {
          "start_time": "11:00:00",
          "direction_id": 1,
          "trip_id": "72040316",
          "route_id": "746",
          "start_date": "20251106",
          "revenue": true,
          "last_trip": false
        }
      }
    }
]
```

We can easily do this by making `pl.Struct` types.

<details open class="code-fold">
<summary>Code</summary>

``` python
from datetime import datetime

structured_sample = (
    sample
    .filter(pl.col("gtfs_arrival_dt").is_not_null() | pl.col("gtfs_departure_dt").is_not_null())
    .sort("gtfs_stop_sequence") # for easier reading
    .with_columns(
        stop_events = pl.struct(
            stop_id = pl.col("stop_id"),
            stop_sequence = pl.col("gtfs_stop_sequence"),
            arrived = pl.col("gtfs_arrival_dt").dt.epoch("s"),
            departed = pl.col("gtfs_departure_dt").dt.epoch("s")
        )
    )
    .group_by(
        pl.concat_str(pl.col("trip_id"), pl.lit("-"), pl.col("vehicle_id")).alias("id"),
    )
    .agg(
        trip = pl.struct([
            pl.col(c).first().alias(c)
            for c in ["start_time", "direction_id", "route_id", "service_date", "trip_id"]
        ]),
        stop_events = pl.implode("stop_events"),
    )
    .select(
        pl.col("id"),
        pl.lit(datetime.now().timestamp()).alias("timestamp"),
        pl.col("trip").struct.with_fields(revenue = pl.lit(True)),
        pl.col("stop_events"),
    )
)
```

</details>

Which then outputs events like:

<details open class="code-fold">
<summary>Code</summary>

``` python
import json

print(
    json.dumps( # output as string, controlling indentation
        json.loads( # load as dict
            structured_sample.head(1).write_ndjson() # output as string but can't control indent
        ),
        indent = 2
    )
)
```

</details>

    {
      "id": "71987884-y0815",
      "timestamp": 1762447374.908796,
      "trip": {
        "start_time": 48360,
        "direction_id": 1,
        "route_id": "216",
        "service_date": "2025-11-04",
        "trip_id": "71987884",
        "revenue": true
      },
      "stop_events": [
        {
          "stop_id": "3265",
          "stop_sequence": 1,
          "arrived": 1762280207,
          "departed": 1762280767
        },
        {
          "stop_id": "3266",
          "stop_sequence": 2,
          "arrived": 1762280773,
          "departed": 1762280779
        },
        {
          "stop_id": "3267",
          "stop_sequence": 3,
          "arrived": 1762280785,
          "departed": 1762280785
        },
        {
          "stop_id": "3268",
          "stop_sequence": 4,
          "arrived": 1762280791,
          "departed": 1762280803
        },
        {
          "stop_id": "32681",
          "stop_sequence": 5,
          "arrived": 1762280809,
          "departed": 1762280815
        },
        {
          "stop_id": "3269",
          "stop_sequence": 6,
          "arrived": 1762280833,
          "departed": 1762280833
        },
        {
          "stop_id": "3270",
          "stop_sequence": 7,
          "arrived": 1762280853,
          "departed": 1762280853
        },
        {
          "stop_id": "3271",
          "stop_sequence": 8,
          "arrived": 1762280859,
          "departed": 1762280865
        },
        {
          "stop_id": "3272",
          "stop_sequence": 9,
          "arrived": 1762280873,
          "departed": 1762280879
        },
        {
          "stop_id": "3273",
          "stop_sequence": 10,
          "arrived": 1762280884,
          "departed": 1762280896
        },
        {
          "stop_id": "3276",
          "stop_sequence": 12,
          "arrived": 1762280932,
          "departed": 1762280932
        },
        {
          "stop_id": "32760",
          "stop_sequence": 13,
          "arrived": 1762280932,
          "departed": 1762280932
        },
        {
          "stop_id": "3277",
          "stop_sequence": 14,
          "arrived": 1762280938,
          "departed": 1762280952
        },
        {
          "stop_id": "3278",
          "stop_sequence": 15,
          "arrived": 1762280999,
          "departed": 1762280999
        },
        {
          "stop_id": "3280",
          "stop_sequence": 17,
          "arrived": 1762281012,
          "departed": 1762281012
        },
        {
          "stop_id": "3281",
          "stop_sequence": 18,
          "arrived": 1762281018,
          "departed": 1762281030
        },
        {
          "stop_id": "3283",
          "stop_sequence": 19,
          "arrived": 1762281061,
          "departed": 1762281069
        },
        {
          "stop_id": "32831",
          "stop_sequence": 20,
          "arrived": 1762281082,
          "departed": 1762281123
        },
        {
          "stop_id": "3286",
          "stop_sequence": 22,
          "arrived": 1762281178,
          "departed": 1762281203
        },
        {
          "stop_id": "3287",
          "stop_sequence": 23,
          "arrived": 1762281259,
          "departed": 1762281271
        },
        {
          "stop_id": "3288",
          "stop_sequence": 24,
          "arrived": 1762281290,
          "departed": 1762281337
        },
        {
          "stop_id": "32910",
          "stop_sequence": 25,
          "arrived": 1762281368,
          "departed": 1762281380
        },
        {
          "stop_id": "32912",
          "stop_sequence": 27,
          "arrived": 1762281416,
          "departed": 1762281479
        },
        {
          "stop_id": "3640",
          "stop_sequence": 28,
          "arrived": 1762281526,
          "departed": 1762281597
        },
        {
          "stop_id": "3038",
          "stop_sequence": 29,
          "arrived": 1762281602,
          "departed": 1762281624
        }
      ]
    }

Now, suppose we wanted to serve a file like this—for the whole day—via
our API. What kind of size would we be dealing with? Python provides a
utility to measure the size of a string:

<details open class="code-fold">
<summary>Code</summary>

``` python
import sys

sys.getsizeof(structured_sample.write_json()) / 1024**2 # to megabytes
```

</details>

    21.359686851501465

Remember this is only for bus, so multiply the size by 2.5x to
approximate the added data for heavy, light, and commuter rail. If we
compress this file, we can get it to:

<details open class="code-fold">
<summary>Code</summary>

``` python
import gzip

sys.getsizeof(
    gzip.compress(
        structured_sample.write_json().encode("utf-8")
    )
) / 1024**2
```

</details>

    2.927931785583496

The realtime predictions file is still much smaller: from 3MB
uncompressed it drops to 350MB, and that’s for all modes. But that file
is only a few hours. If we took only events with departures or arrivals
that occurred between 7a and 9a, then we could shrink this further.

<details open class="code-fold">
<summary>Code</summary>

``` python
sys.getsizeof(
    gzip.compress(
        structured_sample
        .with_columns(
            pl.col("stop_events").list.filter(
                pl.coalesce(
                    pl.element().struct.field("departed"),
                    pl.element().struct.field("arrived"),
                )
                .is_between(datetime(2025, 11, 4, 7).timestamp(), datetime(2025, 11, 4, 9).timestamp())
            )
        )
        .filter(pl.col("stop_events").list.len() > 0)
        .write_json()
        .encode("utf-8")
    )
) / 1024**2
```

</details>

    0.48584747314453125

That’s not quite the same but it’s in the ballpark, which is probably
what matters. I’ll store this file in the `analysis/flashback` folder to
share it:

<details open class="code-fold">
<summary>Code</summary>

``` python
with gzip.open("mock_stop_events.json.gz", "wt", encoding = "utf-8") as file:
    file.write(
        structured_sample
        .with_columns(
            pl.col("stop_events").list.filter(
                pl.coalesce(
                    pl.element().struct.field("departed"),
                    pl.element().struct.field("arrived"),
                )
                .is_between(datetime(2025, 11, 4, 7).timestamp(), datetime(2025, 11, 4, 9).timestamp())
            )
        )
        .filter(pl.col("stop_events").list.len() > 0)
        .write_json()
    )
```

</details>
