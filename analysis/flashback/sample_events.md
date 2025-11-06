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

    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=09e69b8b-7689-4e20-87c3-0009b99e97d2, process_id=40737, status=started, free_disk_mb=339794, free_mem_pct=23, service_date=2025-11-04
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=977c92ca-093f-48fe-93e7-dbd85c875aa3, process_id=40737, status=started, free_disk_mb=339794, free_mem_pct=23, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=1d573ecb-9a22-4e80-b08c-6a0a70bfabb1, process_id=40737, status=started, free_disk_mb=339794, free_mem_pct=23, file=routes, service_date=2025-11-04
    INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=1d573ecb-9a22-4e80-b08c-6a0a70bfabb1, process_id=40737, status=add_metadata, free_disk_mb=339794, free_mem_pct=23, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=1d573ecb-9a22-4e80-b08c-6a0a70bfabb1, process_id=40737, status=add_metadata, free_disk_mb=339798, free_mem_pct=23, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=gtfs_from_parquet, uuid=1d573ecb-9a22-4e80-b08c-6a0a70bfabb1, process_id=40737, status=complete, free_disk_mb=339798, free_mem_pct=23, duration=0.93, file=routes, service_date=2025-11-04, gtfs_file=s3://mbta-ctd-dataplatform-staging-archive/lamp/gtfs_archive/2025/routes.parquet, gtfs_row_count=398
    INFO:root:parent=unknown, process_name=read_vehicle_positions, uuid=977c92ca-093f-48fe-93e7-dbd85c875aa3, process_id=40737, status=complete, free_disk_mb=339798, free_mem_pct=21, duration=3.11, service_date=2025-11-04, file_count=1, reader_engine=polars
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=09e69b8b-7689-4e20-87c3-0009b99e97d2, process_id=40737, status=add_metadata, free_disk_mb=339798, free_mem_pct=21, service_date=2025-11-04, rows_from_parquet=3563271
    INFO:root:parent=unknown, process_name=position_to_events, uuid=ed55f39e-72d4-4e33-b7f1-dbee1a0bd81c, process_id=40737, status=started, free_disk_mb=339798, free_mem_pct=20, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=ed55f39e-72d4-4e33-b7f1-dbee1a0bd81c, process_id=40737, status=add_metadata, free_disk_mb=339798, free_mem_pct=20, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=position_to_events, uuid=ed55f39e-72d4-4e33-b7f1-dbee1a0bd81c, process_id=40737, status=complete, free_disk_mb=339798, free_mem_pct=20, duration=0.00, valid_records=275038, invalid_records=0
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=09e69b8b-7689-4e20-87c3-0009b99e97d2, process_id=40737, status=add_metadata, free_disk_mb=339798, free_mem_pct=20, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038
    INFO:root:parent=unknown, process_name=generate_gtfs_rt_events, uuid=09e69b8b-7689-4e20-87c3-0009b99e97d2, process_id=40737, status=complete, free_disk_mb=339798, free_mem_pct=20, duration=4.38, service_date=2025-11-04, rows_from_parquet=3563271, events_for_day=275038

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
| "71994084" | "5588" | "111" | 2025-11-04 | 61260 | 2025-11-04 17:01:00 | 3 | 23 | 1 | "y1994" | "1994" | null | 2025-11-04 22:01:07 UTC | 2025-11-04 22:01:07 UTC | 2025-11-04 22:01:24 UTC | 42.412344 | -71.031495 |
| "71465588" | "144" | "9" | 2025-11-04 | 58200 | 2025-11-04 16:10:00 | 3 | 27 | 0 | "y1780" | "1780" | 2025-11-04 21:12:37 UTC | 2025-11-04 21:13:05 UTC | 2025-11-04 21:13:05 UTC | 2025-11-04 21:14:30 UTC | 42.35079 | -71.07459 |
| "71997490" | "2584" | "96" | 2025-11-04 | 27900 | 2025-11-04 07:45:00 | 21 | 28 | 1 | "y1404" | "1404" | 2025-11-04 13:09:06 UTC | 2025-11-04 13:09:16 UTC | 2025-11-04 13:09:22 UTC | 2025-11-04 13:09:31 UTC | 42.392179 | -71.119713 |
| "71987706" | "3722" | "225" | 2025-11-04 | 34500 | 2025-11-04 09:35:00 | 13 | 26 | 1 | "y0808" | "0808" | 2025-11-04 14:38:11 UTC | 2025-11-04 14:38:33 UTC | null | null | 42.233186 | -70.975716 |
| "72094606" | "15618" | "34E" | 2025-11-04 | 24000 | 2025-11-04 06:40:00 | 32 | 80 | 0 | "y1657" | "1657" | 2025-11-04 12:01:27 UTC | 2025-11-04 12:01:39 UTC | 2025-11-04 12:01:39 UTC | 2025-11-04 12:01:45 UTC | 42.244528 | -71.17616 |

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
      "id": "71465892-y1827",
      "timestamp": 1762452874.803778,
      "trip": {
        "start_time": 50460,
        "direction_id": 0,
        "route_id": "1",
        "service_date": "2025-11-04",
        "trip_id": "71465892",
        "revenue": true
      },
      "stop_events": [
        {
          "stop_id": "64",
          "stop_sequence": 1,
          "arrived": 1762281485,
          "departed": 1762282893
        },
        {
          "stop_id": "1",
          "stop_sequence": 2,
          "arrived": 1762282912,
          "departed": 1762282935
        },
        {
          "stop_id": "2",
          "stop_sequence": 3,
          "arrived": 1762282954,
          "departed": 1762282985
        },
        {
          "stop_id": "6",
          "stop_sequence": 4,
          "arrived": 1762283069,
          "departed": 1762283128
        },
        {
          "stop_id": "10003",
          "stop_sequence": 5,
          "arrived": 1762283264,
          "departed": 1762283304
        },
        {
          "stop_id": "10590",
          "stop_sequence": 8,
          "arrived": 1762283656,
          "departed": 1762283718
        },
        {
          "stop_id": "87",
          "stop_sequence": 9,
          "arrived": 1762283771,
          "departed": 1762283847
        },
        {
          "stop_id": "188",
          "stop_sequence": 10,
          "arrived": 1762283949,
          "departed": 1762284037
        },
        {
          "stop_id": "89",
          "stop_sequence": 11,
          "arrived": 1762284045,
          "departed": 1762284111
        },
        {
          "stop_id": "91",
          "stop_sequence": 12,
          "arrived": 1762284159,
          "departed": 1762284261
        },
        {
          "stop_id": "93",
          "stop_sequence": 13,
          "arrived": 1762284470,
          "departed": 1762284567
        },
        {
          "stop_id": "95",
          "stop_sequence": 14,
          "arrived": 1762284670,
          "departed": 1762284692
        },
        {
          "stop_id": "97",
          "stop_sequence": 15,
          "arrived": 1762284801,
          "departed": 1762284873
        },
        {
          "stop_id": "99",
          "stop_sequence": 16,
          "arrived": 1762284973,
          "departed": 1762285030
        },
        {
          "stop_id": "101",
          "stop_sequence": 17,
          "arrived": 1762285069,
          "departed": 1762285119
        },
        {
          "stop_id": "102",
          "stop_sequence": 18,
          "arrived": 1762285228,
          "departed": 1762285298
        },
        {
          "stop_id": "104",
          "stop_sequence": 19,
          "arrived": 1762285325,
          "departed": 1762285355
        },
        {
          "stop_id": "106",
          "stop_sequence": 20,
          "arrived": 1762285375,
          "departed": 1762285431
        },
        {
          "stop_id": "107",
          "stop_sequence": 21,
          "arrived": 1762285437,
          "departed": 1762285461
        },
        {
          "stop_id": "108",
          "stop_sequence": 22,
          "arrived": 1762285503,
          "departed": 1762285539
        },
        {
          "stop_id": "109",
          "stop_sequence": 23,
          "arrived": 1762285552,
          "departed": 1762285591
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

    2.9281177520751953

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

    0.48502540588378906

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
        "direction_id": 1,
        "stop_sequence": 3,
        "revenue": "REVENUE",
        "arrived": "2025-11-04T22:01:07+00:00",
        "departed": "2025-11-04T22:01:24+00:00"
      },
      "relationships": {
        "route": {
          "data": {
            "id": "111",
            "type": "route"
          }
        },
        "vehicle": {
          "data": {
            "id": "y1994",
            "type": "vehicle"
          }
        },
        "stop": {
          "data": {
            "id": "5588",
            "type": "stop"
          }
        },
        "trip": {
          "data": {
            "id": "71994084",
            "type": "trip"
          }
        }
      },
      "type": "stop_event"
    }
