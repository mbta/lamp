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

    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=73dca5ca-c2ee-4f8f-b4a1-969a6ccf7914, process_id=61897, status=started, free_disk_mb=337511, free_mem_pct=16, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=d4363de5-36b4-49b5-a30c-24da4614667c, process_id=61897, status=started, free_disk_mb=337511, free_mem_pct=16, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=9a15a39d-2103-4d94-8d94-4f4ceafbae1e, process_id=61897, status=started, free_disk_mb=337511, free_mem_pct=16, file=routes, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=9a15a39d-2103-4d94-8d94-4f4ceafbae1e, process_id=61897, status=add_metadata, free_disk_mb=337511, free_mem_pct=16, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=9a15a39d-2103-4d94-8d94-4f4ceafbae1e, process_id=61897, status=add_metadata, free_disk_mb=337511, free_mem_pct=17, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=9a15a39d-2103-4d94-8d94-4f4ceafbae1e, process_id=61897, status=complete, free_disk_mb=337511, free_mem_pct=17, duration=1.07, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=d4363de5-36b4-49b5-a30c-24da4614667c, process_id=61897, status=complete, free_disk_mb=337511, free_mem_pct=20, duration=3.45, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=73dca5ca-c2ee-4f8f-b4a1-969a6ccf7914, process_id=61897, status=add_metadata, free_disk_mb=337511, free_mem_pct=20, service_date=2025-11-04, rows_from_parquet=3563271
    INFO:root:parent=unknown, process_name=position_to_events, uuid=9d597501-28f4-4a34-bd43-c42136c38e7d, process_id=61897, status=started, free_disk_mb=337511, free_mem_pct=18, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=9d597501-28f4-4a34-bd43-c42136c38e7d, process_id=61897, status=add_metadata, free_disk_mb=337511, free_mem_pct=18, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=9d597501-28f4-4a34-bd43-c42136c38e7d, process_id=61897, status=complete, free_disk_mb=337511, free_mem_pct=18, duration=0.00, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=73dca5ca-c2ee-4f8f-b4a1-969a6ccf7914, process_id=61897, status=add_metadata, free_disk_mb=337511, free_mem_pct=18, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=73dca5ca-c2ee-4f8f-b4a1-969a6ccf7914, process_id=61897, status=complete, free_disk_mb=337511, free_mem_pct=18, duration=4.50, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038

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
| "71466618" | "1510" | "15" | 2025-11-04 | 45960 | 2025-11-04 12:46:00 | 20 | 30 | 0 | "y1834" | "1834" | 2025-11-04 18:13:24 UTC | 2025-11-04 18:13:42 UTC | 2025-11-04 18:13:42 UTC | 2025-11-04 18:14:22 UTC | 42.309814 | -71.063715 |
| "71997686" | "6283" | "108" | 2025-11-04 | 37800 | 2025-11-04 10:30:00 | 2 | 39 | 1 | "y1411" | "1411" | 2025-11-04 15:30:10 UTC | 2025-11-04 15:30:22 UTC | 2025-11-04 15:30:22 UTC | 2025-11-04 15:30:28 UTC | 42.432846 | -71.026932 |
| "72093879" | "16434" | "30" | 2025-11-04 | 31500 | 2025-11-04 08:45:00 | 14 | 26 | 0 | "y1682" | "1682" | 2025-11-04 13:58:56 UTC | 2025-11-04 14:00:28 UTC | 2025-11-04 14:00:33 UTC | 2025-11-04 14:01:06 UTC | 42.27992 | -71.11891 |
| "71372951" | "1365" | "66" | 2025-11-04 | 42240 | 2025-11-04 11:44:00 | 11 | 33 | 0 | "y3202" | "3202" | 2025-11-04 16:56:01 UTC | 2025-11-04 16:56:57 UTC | 2025-11-04 16:56:57 UTC | 2025-11-04 16:57:17 UTC | 42.333385 | -71.106552 |
| "71826067" | "5666" | "120" | 2025-11-04 | 40800 | 2025-11-04 11:20:00 | 10 | 35 | 0 | "y3331" | "3331" | 2025-11-04 16:30:21 UTC | 2025-11-04 16:30:52 UTC | 2025-11-04 16:30:55 UTC | 2025-11-04 16:31:30 UTC | 42.380564 | -71.023832 |

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

## Example Flashback output

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
      "id": "71997479-y1434",
      "timestamp": 1762458979.508543,
      "trip": {
        "start_time": 56640,
        "direction_id": 1,
        "route_id": "411",
        "service_date": "2025-11-04",
        "trip_id": "71997479",
        "revenue": true
      },
      "stop_events": [
        {
          "stop_id": "4761",
          "stop_sequence": 1,
          "arrived": 1762287722,
          "departed": 1762289006
        },
        {
          "stop_id": "44762",
          "stop_sequence": 2,
          "arrived": 1762289052,
          "departed": 1762289064
        },
        {
          "stop_id": "5699",
          "stop_sequence": 3,
          "arrived": 1762289118,
          "departed": 1762289128
        },
        {
          "stop_id": "15799",
          "stop_sequence": 4,
          "arrived": 1762289284,
          "departed": 1762289376
        },
        {
          "stop_id": "15782",
          "stop_sequence": 5,
          "arrived": 1762289461,
          "departed": 1762289484
        },
        {
          "stop_id": "5783",
          "stop_sequence": 6,
          "arrived": 1762289552,
          "departed": 1762289574
        },
        {
          "stop_id": "5784",
          "stop_sequence": 7,
          "arrived": 1762289612,
          "departed": 1762289648
        },
        {
          "stop_id": "4733",
          "stop_sequence": 8,
          "arrived": 1762289691,
          "departed": 1762289804
        },
        {
          "stop_id": "5786",
          "stop_sequence": 9,
          "arrived": 1762289843,
          "departed": 1762289865
        },
        {
          "stop_id": "15787",
          "stop_sequence": 10,
          "arrived": 1762289891,
          "departed": 1762289910
        },
        {
          "stop_id": "5788",
          "stop_sequence": 11,
          "arrived": 1762289922,
          "departed": 1762289966
        },
        {
          "stop_id": "57641",
          "stop_sequence": 12,
          "arrived": 1762290053,
          "departed": 1762290183
        },
        {
          "stop_id": "5765",
          "stop_sequence": 13,
          "arrived": 1762290208,
          "departed": 1762290242
        },
        {
          "stop_id": "35765",
          "stop_sequence": 14,
          "arrived": 1762290249,
          "departed": 1762290254
        },
        {
          "stop_id": "6250",
          "stop_sequence": 15,
          "arrived": 1762290349,
          "departed": 1762290374
        },
        {
          "stop_id": "6251",
          "stop_sequence": 16,
          "arrived": 1762290388,
          "departed": 1762290388
        },
        {
          "stop_id": "6252",
          "stop_sequence": 17,
          "arrived": 1762290395,
          "departed": 1762290415
        },
        {
          "stop_id": "6253",
          "stop_sequence": 18,
          "arrived": 1762290422,
          "departed": 1762290438
        },
        {
          "stop_id": "6254",
          "stop_sequence": 19,
          "arrived": 1762290438,
          "departed": 1762290478
        },
        {
          "stop_id": "6255",
          "stop_sequence": 20,
          "arrived": 1762290478,
          "departed": 1762290497
        },
        {
          "stop_id": "6256",
          "stop_sequence": 21,
          "arrived": 1762290504,
          "departed": 1762290514
        },
        {
          "stop_id": "6257",
          "stop_sequence": 22,
          "arrived": 1762290530,
          "departed": 1762290558
        },
        {
          "stop_id": "6258",
          "stop_sequence": 23,
          "arrived": 1762290573,
          "departed": 1762290594
        },
        {
          "stop_id": "6259",
          "stop_sequence": 24,
          "arrived": 1762290601,
          "departed": 1762290619
        },
        {
          "stop_id": "6260",
          "stop_sequence": 25,
          "arrived": 1762290645,
          "departed": 1762290668
        },
        {
          "stop_id": "6262",
          "stop_sequence": 26,
          "arrived": 1762290701,
          "departed": 1762290737
        },
        {
          "stop_id": "7414",
          "stop_sequence": 27,
          "arrived": 1762290893,
          "departed": 1762290934
        },
        {
          "stop_id": "45534",
          "stop_sequence": 28,
          "arrived": 1762291153,
          "departed": 1762291185
        },
        {
          "stop_id": "6267",
          "stop_sequence": 29,
          "arrived": 1762291263,
          "departed": 1762291375
        },
        {
          "stop_id": "7415",
          "stop_sequence": 30,
          "arrived": 1762291518,
          "departed": 1762291518
        },
        {
          "stop_id": "7417",
          "stop_sequence": 31,
          "arrived": 1762291565,
          "departed": 1762291602
        },
        {
          "stop_id": "7418",
          "stop_sequence": 32,
          "arrived": 1762291608,
          "departed": 1762291616
        },
        {
          "stop_id": "7419",
          "stop_sequence": 33,
          "arrived": 1762291623,
          "departed": 1762291651
        },
        {
          "stop_id": "46267",
          "stop_sequence": 34,
          "arrived": 1762291695,
          "departed": 1762291710
        },
        {
          "stop_id": "46268",
          "stop_sequence": 35,
          "arrived": 1762291757,
          "departed": 1762291786
        },
        {
          "stop_id": "8336",
          "stop_sequence": 36,
          "arrived": 1762291847,
          "departed": 1762291879
        },
        {
          "stop_id": "8338",
          "stop_sequence": 37,
          "arrived": 1762291888,
          "departed": 1762291911
        },
        {
          "stop_id": "18338",
          "stop_sequence": 38,
          "arrived": 1762291916,
          "departed": 1762291976
        },
        {
          "stop_id": "17463",
          "stop_sequence": 39,
          "arrived": 1762291979,
          "departed": 1762291989
        },
        {
          "stop_id": "8576",
          "stop_sequence": 40,
          "arrived": 1762291998,
          "departed": 1762292009
        },
        {
          "stop_id": "8577",
          "stop_sequence": 41,
          "arrived": 1762292032,
          "departed": 1762292084
        },
        {
          "stop_id": "7536",
          "stop_sequence": 42,
          "arrived": 1762292091,
          "departed": 1762292126
        },
        {
          "stop_id": "7537",
          "stop_sequence": 43,
          "arrived": 1762292145,
          "departed": 1762292180
        },
        {
          "stop_id": "7539",
          "stop_sequence": 44,
          "arrived": 1762292195,
          "departed": 1762292210
        },
        {
          "stop_id": "7540",
          "stop_sequence": 45,
          "arrived": 1762292227,
          "departed": 1762292305
        },
        {
          "stop_id": "9021",
          "stop_sequence": 46,
          "arrived": 1762292305,
          "departed": 1762292335
        },
        {
          "stop_id": "9023",
          "stop_sequence": 47,
          "arrived": 1762292353,
          "departed": 1762292387
        },
        {
          "stop_id": "9024",
          "stop_sequence": 48,
          "arrived": 1762292393,
          "departed": 1762292429
        },
        {
          "stop_id": "9025",
          "stop_sequence": 49,
          "arrived": 1762292443,
          "departed": 1762292508
        },
        {
          "stop_id": "9026",
          "stop_sequence": 50,
          "arrived": 1762292530,
          "departed": 1762292559
        },
        {
          "stop_id": "5440",
          "stop_sequence": 51,
          "arrived": 1762292587,
          "departed": 1762292601
        },
        {
          "stop_id": "5441",
          "stop_sequence": 52,
          "arrived": 1762292611,
          "departed": 1762292630
        },
        {
          "stop_id": "5442",
          "stop_sequence": 53,
          "arrived": 1762292658,
          "departed": 1762292706
        },
        {
          "stop_id": "5443",
          "stop_sequence": 54,
          "arrived": 1762292715,
          "departed": 1762292763
        },
        {
          "stop_id": "5444",
          "stop_sequence": 55,
          "arrived": 1762292782,
          "departed": 1762292782
        },
        {
          "stop_id": "5445",
          "stop_sequence": 56,
          "arrived": 1762292801,
          "departed": 1762292873
        },
        {
          "stop_id": "15431",
          "stop_sequence": 57,
          "arrived": 1762292931,
          "departed": 1762292956
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

    2.9295339584350586

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

    0.48604297637939453

That’s not quite the same but it’s in the ballpark, which is probably
what matters. I’ll store this file (uncompressed) in the
`analysis/flashback` folder to share it:

<details open class="code-fold">
<summary>Code</summary>

``` python
(
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
    .write_json("mock_stop_events.json")
)
```

</details>

This is the format LAMP would output for the API. Concentrate would then
turn this file into an endpoint that returned responses like the
`predictions` endpoint:

``` json
{
    "attributes": {
        "arrival_time": null,
        "arrival_uncertainty": null,
        "departure_time": null,
        "departure_uncertainty": null,
        "direction_id": 0,
        "last_trip": false,
        "revenue": "REVENUE",
        "schedule_relationship": "CANCELLED",
        "status": null,
        "stop_sequence": 2,
        "update_type": null
    },
    "id": "prediction-71468273-1-2-1",
    "relationships": {
    "route": {
        "data": {
            "id": "1",
            "type": "route"
        }
    },
    "stop": {
        "data": {
            "id": "1",
            "type": "stop"
        }
    },
    "trip": {
        "data": {
            "id": "71468273",
            "type": "trip"
        }
    },
    "vehicle": {
        "data": null
    }
    },
    "type": "prediction"
}
```

For us, the data would look like:

## Example API response

<details open class="code-fold">
<summary>Code</summary>

``` python
print(
    json.dumps(
        json.loads(
            (
                sample
                .select(
                    attributes = pl.struct(
                        direction_id = pl.col("direction_id"),
                        stop_sequence = pl.col("gtfs_stop_sequence"),
                        revenue = pl.lit("REVENUE"),
                        arrived = pl.col("gtfs_arrival_dt"),
                        departed = pl.col("gtfs_departure_dt"),
                    ),
                    relationships = pl.struct(
                        [
                            pl.struct(
                                data = pl.struct(
                                    id = pl.col(f"{c}_id"),
                                    type = pl.lit(c),
                                )
                            ).alias(c) for c in ["route", "vehicle", "stop", "trip"]
                        ]
                    ),
                    type = pl.lit("stop_event"),
                )
                .head(1)
                .write_ndjson()
            )
        ), indent = 2
    )
)
```

</details>

    {
      "attributes": {
        "direction_id": 0,
        "stop_sequence": 20,
        "revenue": "REVENUE",
        "arrived": "2025-11-04T18:13:42+00:00",
        "departed": "2025-11-04T18:14:22+00:00"
      },
      "relationships": {
        "route": {
          "data": {
            "id": "15",
            "type": "route"
          }
        },
        "vehicle": {
          "data": {
            "id": "y1834",
            "type": "vehicle"
          }
        },
        "stop": {
          "data": {
            "id": "1510",
            "type": "stop"
          }
        },
        "trip": {
          "data": {
            "id": "71466618",
            "type": "trip"
          }
        }
      },
      "type": "stop_event"
    }
