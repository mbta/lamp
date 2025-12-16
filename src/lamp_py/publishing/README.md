# How to use Lightswitch


- [Configuration](#configuration)
- [Querying](#querying)

LAMP’s metastore (data catalog?) provides users the experience of
querying a relational database without requiring all the overhead that
databases require (for the LAMP and Infra teams 🙂). That means a query
that used to look like this:

``` sql
SELECT *
FROM read_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/year=2025/month=9/day=26/2025-09-26T00:00:00.parquet')
LIMIT 10
```

Now can be written as:

``` sql
SELECT *
FROM lamp.BUS_VEHICLE_POSITIONS
WHERE year = 2025
AND month = 9
AND day = 26
LIMIT 10
```

## Configuration

First, pick a DuckDB interface. As a default, stick with the included
[DuckDB UI](https://duckdb.org/docs/stable/core_extensions/ui) for a
minimal notebook. Other options are abundant but a few stand out:

- [DuckDB CLI](https://duckdb.org/docs/stable/clients/cli/overview.html)
  for maximum extensibility
- [DuckDB shell](https://shell.duckdb.org/) for a browser-based
  experience
- [marimo](https://marimo.io/) for a shiny notebook experience
- [DBeaver](https://dbeaver.io/) if you want the feel of an old SQL
  editor

(This document is rendered by R’s `duckdb` library and
[Quarto](https://quarto.org/), which provides options for different
outputs like websites, presentations, and PDFs.)

To access the s3 buckets that hold LAMP data, you need to authenticate
using their IAM account, which DuckDB supports natively via the DuckDB
`aws` extension.

``` sql
INSTALL aws;
```

I have already [authenticated using
`awscli`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-methods)
so I can tell DuckDB to use those credentials. If you want to directly
use an access key or set up a different authentication method, look at
[DuckDB’s
docs](https://duckdb.org/docs/stable/core_extensions/aws.html). Since
DuckDB doesn’t persist credentials between sessions, you’ll need to
**run these next lines each time you start a DuckDB session**.

``` sql
LOAD aws;
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
);
```

| Success |
|:--------|
| TRUE    |

1 records

Once authenticated, attach the Lightswitch data catalog. This is a
DuckDB database that only holds views of LAMP Parquet URIs. For
instance, the view for `RT_VEHICLE_POSITIONS` contains logic that lists
the URLs for each `RT_VEHICLE_POSITIONS` file in LAMP’s springboard
bucket.

``` sql
ATTACH 's3://mbta-ctd-dataplatform-archive/lamp/catalog.db' AS lamp
```

And that’s it! You’re all set up.

## Querying

Get familiar with what’s available by listing the database’s views:

``` sql
SHOW TABLES FROM lamp
```

| name                           |
|:-------------------------------|
| BUS_TRIP_UPDATES               |
| BUS_VEHICLE_POSITIONS          |
| DAILY_LOGGED_MESSAGE           |
| DAILY_SCHED_ADHERE_WAIVER      |
| DAILY_WORK_PIECE               |
| DEV_GREEN_LRTP_TRIP_UPDATES    |
| DEV_GREEN_RT_TRIP_UPDATES      |
| DEV_GREEN_RT_VEHICLE_POSITIONS |
| LAMP_ALL_Bus_Events            |
| LAMP_ALL_Bus_Operator_Mapping  |

Displaying records 1 - 10

GTFS-RT data is partitioned by `year`, `month`, and `day` and filtering
the view by the partition is key to performance. Let’s take
`rt_vehicle_positions` for instance.

``` sql
DESCRIBE lamp.rt_vehicle_positions
```

| column_name                        | column_type | null | key | default | extra |
|:-----------------------------------|:------------|:-----|:----|:--------|:------|
| id                                 | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.trip_id               | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.route_id              | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.direction_id          | UTINYINT    | YES  | NA  | NA      | NA    |
| vehicle.trip.start_time            | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.start_date            | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.schedule_relationship | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.route_pattern_id      | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.tm_trip_id            | VARCHAR     | YES  | NA  | NA      | NA    |
| vehicle.trip.overload_id           | BIGINT      | YES  | NA  | NA      | NA    |

Displaying records 1 - 10

To query this view, apply a filter on `year`, `month`, and `day`:

``` sql
SELECT *
FROM lamp.rt_vehicle_positions
WHERE year = 2025
AND month = 10
AND day BETWEEN 1 AND 9
```

| id | vehicle.trip.trip_id | vehicle.trip.route_id | vehicle.trip.direction_id | vehicle.trip.start_time | vehicle.trip.start_date | vehicle.trip.schedule_relationship | vehicle.trip.route_pattern_id | vehicle.trip.tm_trip_id | vehicle.trip.overload_id | vehicle.trip.overload_offset | vehicle.trip.revenue | vehicle.vehicle.id | vehicle.vehicle.label | vehicle.vehicle.license_plate | vehicle.vehicle.consist | vehicle.vehicle.assignment_status | vehicle.position.bearing | vehicle.position.latitude | vehicle.position.longitude | vehicle.position.speed | vehicle.position.odometer | vehicle.current_stop_sequence | vehicle.stop_id | vehicle.current_status | vehicle.timestamp | vehicle.congestion_level | vehicle.occupancy_status | vehicle.occupancy_percentage | vehicle.multi_carriage_details | feed_timestamp | day | month | year |
|:---|:---|:---|---:|:---|:---|:---|:---|:---|---:|---:|:---|:---|:---|:---|:---|:---|---:|---:|---:|---:|---:|---:|:---|:---|---:|:---|:---|---:|:---|---:|---:|---:|---:|
| y1850 | 71466788 | 10 | 0 | 19:40:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1850 | 1850 | NA | NULL | NA | 100 | 42.32979 | -71.05699 | NA | NA | 21 | 14 | IN_TRANSIT_TO | 1759277122 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759277128 | 1 | 10 | 2025 |
| y1735 | 71466787 | 10 | 0 | 19:20:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1735 | 1735 | NA | NULL | NA | 347 | 42.33596 | -71.02505 | NA | NA | 36 | 30 | STOPPED_AT | 1759276842 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759276848 | 1 | 10 | 2025 |
| y1820 | 71465779 | 10 | 1 | 19:51:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1820 | 1820 | NA | NULL | NA | 110 | 42.32995 | -71.05720 | NA | NA | 18 | 13 | STOPPED_AT | 1759277255 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277261 | 1 | 10 | 2025 |
| y1740 | 71467102 | 10 | 1 | 19:20:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1740 | 1740 | NA | NULL | NA | 90 | 42.35185 | -71.07100 | NA | NA | 36 | 177 | IN_TRANSIT_TO | 1759277205 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277213 | 1 | 10 | 2025 |
| y1820 | 71465779 | 10 | 1 | 19:51:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1820 | 1820 | NA | NULL | NA | 270 | 42.33540 | -71.04599 | NA | NA | 11 | 46 | STOPPED_AT | 1759277047 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277052 | 1 | 10 | 2025 |
| y1820 | 71465779 | 10 | 1 | 19:51:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1820 | 1820 | NA | NULL | NA | 270 | 42.33547 | -71.04529 | NA | NA | 11 | 46 | IN_TRANSIT_TO | 1759277032 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277035 | 1 | 10 | 2025 |
| y1740 | 71467102 | 10 | 1 | 19:20:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1740 | 1740 | NA | NULL | NA | 342 | 42.34566 | -71.07519 | NA | NA | 34 | 11384 | IN_TRANSIT_TO | 1759276824 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759276831 | 1 | 10 | 2025 |
| y1740 | 71467102 | 10 | 1 | 19:20:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1740 | 1740 | NA | NULL | NA | 340 | 42.35060 | -71.07285 | NA | NA | 35 | 144 | IN_TRANSIT_TO | 1759277093 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277096 | 1 | 10 | 2025 |
| y1740 | 71467102 | 10 | 1 | 19:20:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1740 | 1740 | NA | NULL | NA | 73 | 42.35177 | -71.07102 | NA | NA | 36 | 177 | IN_TRANSIT_TO | 1759277190 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277194 | 1 | 10 | 2025 |
| y1820 | 71465779 | 10 | 1 | 19:51:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | y1820 | 1820 | NA | NULL | NA | 269 | 42.33558 | -71.04147 | NA | NA | 9 | 44 | IN_TRANSIT_TO | 1759276903 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759276909 | 1 | 10 | 2025 |

Displaying records 1 - 10

⚠️ Unfortunately, DuckDB doesn’t recognize `year`, `month`, and `day` as
composing a date, so this query won’t work:

``` sql
SELECT *
FROM lamp.rt_vehicle_positions
WHERE file_date BETWEEN '2025-10-01' and '2025-10-09'
```

In addition, the native syntax is rather slow. To more efficiently query
year-month-day partitioned datasets such as `rt_vehicle_positions`, use
the function `lamp.read_ymd`, which accepts start and end dates.

``` sql
SELECT *
FROM lamp.read_ymd(
    "RT_VEHICLE_POSITIONS", -- case sensitive
    DATE '2025-10-01',
    DATE '2025-10-10' -- end date is not inclusive
)
LIMIT 10
```

| id | vehicle.trip.trip_id | vehicle.trip.route_id | vehicle.trip.direction_id | vehicle.trip.start_time | vehicle.trip.start_date | vehicle.trip.schedule_relationship | vehicle.trip.route_pattern_id | vehicle.trip.tm_trip_id | vehicle.trip.overload_id | vehicle.trip.overload_offset | vehicle.trip.revenue | vehicle.trip.last_trip | vehicle.vehicle.id | vehicle.vehicle.label | vehicle.vehicle.license_plate | vehicle.vehicle.consist | vehicle.vehicle.assignment_status | vehicle.position.bearing | vehicle.position.latitude | vehicle.position.longitude | vehicle.position.speed | vehicle.position.odometer | vehicle.current_stop_sequence | vehicle.stop_id | vehicle.current_status | vehicle.timestamp | vehicle.congestion_level | vehicle.occupancy_status | vehicle.occupancy_percentage | vehicle.multi_carriage_details | feed_timestamp | day | month | year |
|:---|:---|:---|---:|:---|:---|:---|:---|:---|---:|---:|:---|:---|:---|:---|:---|:---|:---|---:|---:|---:|---:|---:|---:|:---|:---|---:|:---|:---|---:|:---|---:|---:|---:|---:|
| y2078 | 71996451 | 112 | 0 | 19:25:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2078 | 2078 | NA | NULL | NA | 225 | 42.40531 | -71.05716 | NA | NA | 42 | 5559 | STOPPED_AT | 1759276974 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759276984 | 1 | 10 | 2025 |
| y2078 | 71996451 | 112 | 0 | 19:25:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2078 | 2078 | NA | NULL | NA | 270 | 42.40266 | -71.06282 | NA | NA | 44 | 5561 | IN_TRANSIT_TO | 1759277049 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277052 | 1 | 10 | 2025 |
| y2078 | 71996451 | 112 | 0 | 19:25:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2078 | 2078 | NA | NULL | NA | 315 | 42.40573 | -71.05298 | NA | NA | 39 | 5692 | IN_TRANSIT_TO | 1759276851 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759276858 | 1 | 10 | 2025 |
| y2050 | 71993401 | 112 | 1 | 19:08:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2050 | 2050 | NA | NULL | NA | 90 | 42.38778 | -71.02392 | NA | NA | 42 | 5670 | IN_TRANSIT_TO | 1759277116 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759277120 | 1 | 10 | 2025 |
| y0893 | 71996503 | 112 | 1 | 19:50:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y0893 | 0893 | NA | NULL | NA | 116 | 42.40246 | -71.03598 | NA | NA | 13 | 15649 | IN_TRANSIT_TO | 1759276998 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759277005 | 1 | 10 | 2025 |
| y0893 | 71996503 | 112 | 1 | 19:50:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y0893 | 0893 | NA | NULL | NA | 180 | 42.40109 | -71.02845 | NA | NA | 15 | 15651 | STOPPED_AT | 1759277099 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277102 | 1 | 10 | 2025 |
| y2078 | 71996451 | 112 | 0 | 19:25:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2078 | 2078 | NA | NULL | NA | 315 | 42.40444 | -71.07626 | NA | NA | 46 | 52720 | IN_TRANSIT_TO | 1759277132 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277144 | 1 | 10 | 2025 |
| y0893 | 71996503 | 112 | 1 | 19:50:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y0893 | 0893 | NA | NULL | NA | 203 | 42.40255 | -71.03514 | NA | NA | 16 | 5624 | IN_TRANSIT_TO | 1759277193 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277197 | 1 | 10 | 2025 |
| y0893 | 71996503 | 112 | 1 | 19:50:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y0893 | 0893 | NA | NULL | NA | 143 | 42.40177 | -71.04151 | NA | NA | 12 | 5597 | IN_TRANSIT_TO | 1759276914 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759276920 | 1 | 10 | 2025 |
| y2078 | 71996451 | 112 | 0 | 19:25:00 | 20250930 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2078 | 2078 | NA | NULL | NA | 225 | 42.40263 | -71.06163 | NA | NA | 44 | 5561 | IN_TRANSIT_TO | 1759277039 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759277043 | 1 | 10 | 2025 |

Displaying records 1 - 10
