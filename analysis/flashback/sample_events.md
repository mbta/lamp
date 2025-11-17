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

    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=c5a2fe29-784f-427d-b0be-6c824b41803d, process_id=39301, status=started, free_disk_mb=329245, free_mem_pct=22, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=a8ab22bf-00c7-4a2d-805c-db18e6b89d83, process_id=39301, status=started, free_disk_mb=329245, free_mem_pct=22, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=0cb7fa63-84b1-492c-a6de-a045486c328c, process_id=39301, status=started, free_disk_mb=329245, free_mem_pct=22, file=routes, service_date=2025-11-04
    INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=0cb7fa63-84b1-492c-a6de-a045486c328c, process_id=39301, status=add_metadata, free_disk_mb=329245, free_mem_pct=22, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=0cb7fa63-84b1-492c-a6de-a045486c328c, process_id=39301, status=add_metadata, free_disk_mb=329245, free_mem_pct=22, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=0cb7fa63-84b1-492c-a6de-a045486c328c, process_id=39301, status=complete, free_disk_mb=329245, free_mem_pct=22, duration=1.66, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=a8ab22bf-00c7-4a2d-805c-db18e6b89d83, process_id=39301, status=complete, free_disk_mb=329243, free_mem_pct=19, duration=7.80, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=c5a2fe29-784f-427d-b0be-6c824b41803d, process_id=39301, status=add_metadata, free_disk_mb=329243, free_mem_pct=19, service_date=2025-11-04, rows_from_parquet=3563271
    INFO:root:parent=unknown, process_name=position_to_events, uuid=b3106d7c-75bd-46d2-91c6-f44c1b923b0d, process_id=39301, status=started, free_disk_mb=329243, free_mem_pct=18, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=b3106d7c-75bd-46d2-91c6-f44c1b923b0d, process_id=39301, status=add_metadata, free_disk_mb=329243, free_mem_pct=18, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=b3106d7c-75bd-46d2-91c6-f44c1b923b0d, process_id=39301, status=complete, free_disk_mb=329243, free_mem_pct=18, duration=0.00, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=c5a2fe29-784f-427d-b0be-6c824b41803d, process_id=39301, status=add_metadata, free_disk_mb=329243, free_mem_pct=18, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=c5a2fe29-784f-427d-b0be-6c824b41803d, process_id=39301, status=complete, free_disk_mb=329243, free_mem_pct=18, duration=8.74, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038

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
| "72094154_2" | "1129" | "39" | 2025-11-04 | 24960 | 2025-11-04 06:56:00 | 19 | 14 | 1 | "y1647" | "1647" | 2025-11-04 11:55:40 UTC | 2025-11-04 11:56:42 UTC | 2025-11-04 11:56:47 UTC | 2025-11-04 11:57:16 UTC | 42.31002 | -71.11522 |
| "72093548" | "10777" | "36" | 2025-11-04 | 34200 | 2025-11-04 09:30:00 | 7 | 32 | 1 | "y1652" | "1652" | 2025-11-04 14:39:52 UTC | 2025-11-04 14:40:13 UTC | 2025-11-04 14:40:13 UTC | 2025-11-04 14:40:22 UTC | 42.276378 | -71.166203 |
| "71988025" | "3405" | "215" | 2025-11-04 | 64200 | 2025-11-04 17:50:00 | 33 | 50 | 0 | "y0761" | "0761" | 2025-11-04 23:09:49 UTC | 2025-11-04 23:10:01 UTC | 2025-11-04 23:10:01 UTC | 2025-11-04 23:10:01 UTC | 42.245583 | -71.028715 |
| "71826332" | "6830" | "455" | 2025-11-04 | 63300 | 2025-11-04 17:35:00 | 25 | 69 | 0 | "y3113" | "3113" | 2025-11-04 23:11:00 UTC | 2025-11-04 23:11:13 UTC | 2025-11-04 23:11:16 UTC | 2025-11-04 23:12:04 UTC | 42.464707 | -70.943457 |
| "71466552" | "334" | "22" | 2025-11-04 | 29820 | 2025-11-04 08:17:00 | 28 | 28 | 0 | "y1865" | "1865" | 2025-11-04 13:55:56 UTC | 2025-11-04 13:57:09 UTC | null | null | 42.285933 | -71.064308 |

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
      "id": "71466305-y1725",
      "timestamp": 1763401003.651427,
      "trip": {
        "start_time": 30120,
        "direction_id": 1,
        "route_id": "15",
        "service_date": "2025-11-04",
        "trip_id": "71466305",
        "revenue": true
      },
      "stop_events": [
        {
          "stop_id": "323",
          "stop_sequence": 1,
          "arrived": 1762261637,
          "departed": 1762262535
        },
        {
          "stop_id": "322",
          "stop_sequence": 2,
          "arrived": 1762262569,
          "departed": 1762262630
        },
        {
          "stop_id": "557",
          "stop_sequence": 3,
          "arrived": 1762262641,
          "departed": 1762262666
        },
        {
          "stop_id": "558",
          "stop_sequence": 4,
          "arrived": 1762262682,
          "departed": 1762262704
        },
        {
          "stop_id": "559",
          "stop_sequence": 5,
          "arrived": 1762262704,
          "departed": 1762262736
        },
        {
          "stop_id": "560",
          "stop_sequence": 6,
          "arrived": 1762262740,
          "departed": 1762262752
        },
        {
          "stop_id": "561",
          "stop_sequence": 7,
          "arrived": 1762262756,
          "departed": 1762262869
        },
        {
          "stop_id": "1468",
          "stop_sequence": 8,
          "arrived": 1762262874,
          "departed": 1762262887
        },
        {
          "stop_id": "1469",
          "stop_sequence": 9,
          "arrived": 1762262899,
          "departed": 1762262911
        },
        {
          "stop_id": "1470",
          "stop_sequence": 10,
          "arrived": 1762262923,
          "departed": 1762262947
        },
        {
          "stop_id": "1471",
          "stop_sequence": 11,
          "arrived": 1762262983,
          "departed": 1762263025
        },
        {
          "stop_id": "1472",
          "stop_sequence": 12,
          "arrived": 1762263039,
          "departed": 1762263094
        },
        {
          "stop_id": "1473",
          "stop_sequence": 13,
          "arrived": 1762263094,
          "departed": 1762263104
        },
        {
          "stop_id": "14731",
          "stop_sequence": 14,
          "arrived": 1762263109,
          "departed": 1762263117
        },
        {
          "stop_id": "1474",
          "stop_sequence": 15,
          "arrived": 1762263130,
          "departed": 1762263140
        },
        {
          "stop_id": "1475",
          "stop_sequence": 16,
          "arrived": 1762263142,
          "departed": 1762263186
        },
        {
          "stop_id": "1478",
          "stop_sequence": 17,
          "arrived": 1762263204,
          "departed": 1762263215
        },
        {
          "stop_id": "1479",
          "stop_sequence": 18,
          "arrived": 1762263245,
          "departed": 1762263283
        },
        {
          "stop_id": "1480",
          "stop_sequence": 19,
          "arrived": 1762263425,
          "departed": 1762263510
        },
        {
          "stop_id": "1481",
          "stop_sequence": 20,
          "arrived": 1762263529,
          "departed": 1762263529
        },
        {
          "stop_id": "11482",
          "stop_sequence": 21,
          "arrived": 1762263606,
          "departed": 1762263695
        },
        {
          "stop_id": "14831",
          "stop_sequence": 22,
          "arrived": 1762263724,
          "departed": 1762263783
        },
        {
          "stop_id": "1484",
          "stop_sequence": 23,
          "arrived": 1762263832,
          "departed": 1762263862
        },
        {
          "stop_id": "1485",
          "stop_sequence": 24,
          "arrived": 1762263889,
          "departed": 1762263972
        },
        {
          "stop_id": "1486",
          "stop_sequence": 25,
          "arrived": 1762264070,
          "departed": 1762264084
        },
        {
          "stop_id": "1487",
          "stop_sequence": 26,
          "arrived": 1762264089,
          "departed": 1762264101
        },
        {
          "stop_id": "1488",
          "stop_sequence": 27,
          "arrived": 1762264116,
          "departed": 1762264131
        },
        {
          "stop_id": "1489",
          "stop_sequence": 28,
          "arrived": 1762264132,
          "departed": 1762264161
        },
        {
          "stop_id": "1491",
          "stop_sequence": 29,
          "arrived": 1762264358,
          "departed": 1762264430
        },
        {
          "stop_id": "64000",
          "stop_sequence": 30,
          "arrived": 1762264446,
          "departed": 1762264487
        },
        {
          "stop_id": "1148",
          "stop_sequence": 31,
          "arrived": 1762264780,
          "departed": 1762264792
        },
        {
          "stop_id": "11149",
          "stop_sequence": 32,
          "arrived": 1762264808,
          "departed": 1762264830
        },
        {
          "stop_id": "11148",
          "stop_sequence": 33,
          "arrived": 1762264833,
          "departed": 1762264837
        },
        {
          "stop_id": "21148",
          "stop_sequence": 34,
          "arrived": 1762264853,
          "departed": 1762264892
        },
        {
          "stop_id": "1224",
          "stop_sequence": 35,
          "arrived": 1762264916,
          "departed": 1762264960
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

    2.929372787475586

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

    0.4855794906616211

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
                    id = pl.concat_str([pl.lit("stop-event"), pl.col("trip_id"), pl.col("vehicle_id"), pl.col("gtfs_stop_sequence")], separator = "-", ignore_nulls = True),
                    attributes = pl.struct(
                        direction_id = pl.col("direction_id"),
                        stop_sequence = pl.col("gtfs_stop_sequence"),
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
      "id": "stop-event-72094154_2-y1647-19",
      "attributes": {
        "direction_id": 1,
        "stop_sequence": 19,
        "arrived": "2025-11-04T11:56:47+00:00",
        "departed": "2025-11-04T11:57:16+00:00"
      },
      "relationships": {
        "route": {
          "data": {
            "id": "39",
            "type": "route"
          }
        },
        "vehicle": {
          "data": {
            "id": "y1647",
            "type": "vehicle"
          }
        },
        "stop": {
          "data": {
            "id": "1129",
            "type": "stop"
          }
        },
        "trip": {
          "data": {
            "id": "72094154_2",
            "type": "trip"
          }
        }
      },
      "type": "stop_event"
    }
