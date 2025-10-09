# How to use LAMP’s metastore
crunkel@mbta.com
2025-10-09

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
FROM read_date_partitioned("BUS_VEHICLE_POSITIONS", DATE '2025-09-26')
LIMIT 10
```

## Configuration

To access the s3 buckets that hold LAMP data, users need to authenticate
using their IAM account, which DuckDB supports natively via the `aws`
extension.

``` sql
INSTALL aws;
LOAD aws;
```

I have already authenticated using `awscli` on my Mac so I can tell
DuckDB to use those credentials. If you want to directly use an access
key or set up a different authentication method, look at [DuckDB’s
docs](https://duckdb.org/docs/stable/core_extensions/aws.html).

``` sql
CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
)
```

Once authenticated, attach the LAMP metastore (LAMPstand?). This is a
DuckDB database in itself that stores mappings for LAMP Parquet URIs.

``` sql
ATTACH 's3://mbta-ctd-dataplatform-dev-archive/lamp/catalog.db' AS lamp
```

And that’s it! You’re all set up.

## Querying

Get familiar with what’s available by listing the database’s views:

``` sql
SHOW TABLES FROM lamp
```

| name     |
|:---------|
| calendar |
| shapes   |

2 records

Most of the LAMP data is partitioned by time and filtering the view by
the partition is key to performance and ensuring relevant data. Let’s
take `shapes` for instance.

``` sql
DESCRIBE lamp.shapes
```

| column_name         | column_type | null | key | default | extra |
|:--------------------|:------------|:-----|:----|:--------|:------|
| shape_id            | VARCHAR     | YES  | NA  | NA      | NA    |
| shape_pt_lat        | DOUBLE      | YES  | NA  | NA      | NA    |
| shape_pt_lon        | DOUBLE      | YES  | NA  | NA      | NA    |
| shape_pt_sequence   | BIGINT      | YES  | NA  | NA      | NA    |
| shape_dist_traveled | INTEGER     | YES  | NA  | NA      | NA    |
| timestamp           | BIGINT      | YES  | NA  | NA      | NA    |

6 records

After the column definitions, DuckDB tells us that this dataset is
partitioned on `timestamp`. To query this view, apply a filter on that
`timestamp` field:

``` sql
SELECT *
FROM lamp.shapes
WHERE to_timestamp(timestamp) >= make_timestamp(2025, 10, 1, 0, 0, 0)
```

| shape_id | shape_pt_lat | shape_pt_lon | shape_pt_sequence | shape_dist_traveled | timestamp |
|:---|---:|---:|---:|---:|---:|
| 010140 | 42.37304 | -71.11769 | 10001 | NA | 1759420411 |
| 010140 | 42.37310 | -71.11791 | 10002 | NA | 1759420411 |
| 010140 | 42.37327 | -71.11846 | 10003 | NA | 1759420411 |
| 010140 | 42.37335 | -71.11858 | 10004 | NA | 1759420411 |
| 010140 | 42.37324 | -71.11876 | 10005 | NA | 1759420411 |
| 010140 | 42.37300 | -71.11888 | 10006 | NA | 1759420411 |
| 010140 | 42.37258 | -71.11911 | 10007 | NA | 1759420411 |
| 010140 | 42.37224 | -71.11935 | 10008 | NA | 1759420411 |
| 010140 | 42.37211 | -71.11901 | 10009 | NA | 1759420411 |
| 010140 | 42.37196 | -71.11861 | 10010 | NA | 1759420411 |

Displaying records 1 - 10

The LAMP metastore (LAMP shop?) provides access to realtime datasets,
which are partitioned by year, month, and day, differently due to their
size. Instead of querying a view, query a function:

``` sql
SELECT *
FROM lamp.read_date_partitioned("RT_VEHICLE_POSITIONS", [current_date])
LIMIT 10
```

| id | vehicle.trip.trip_id | vehicle.trip.route_id | vehicle.trip.direction_id | vehicle.trip.start_time | vehicle.trip.start_date | vehicle.trip.schedule_relationship | vehicle.trip.route_pattern_id | vehicle.trip.tm_trip_id | vehicle.trip.overload_id | vehicle.trip.overload_offset | vehicle.trip.revenue | vehicle.trip.last_trip | vehicle.vehicle.id | vehicle.vehicle.label | vehicle.vehicle.license_plate | vehicle.vehicle.consist | vehicle.vehicle.assignment_status | vehicle.position.bearing | vehicle.position.latitude | vehicle.position.longitude | vehicle.position.speed | vehicle.position.odometer | vehicle.current_stop_sequence | vehicle.stop_id | vehicle.current_status | vehicle.timestamp | vehicle.congestion_level | vehicle.occupancy_status | vehicle.occupancy_percentage | vehicle.multi_carriage_details | feed_timestamp | day | month | year |
|:---|:---|:---|---:|:---|:---|:---|:---|:---|---:|---:|:---|:---|:---|:---|:---|:---|:---|---:|---:|---:|---:|---:|---:|:---|:---|---:|:---|:---|---:|:---|---:|---:|---:|---:|
| y1935 | 71694900 | 77 | 1 | 20:01:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y1935 | 1935 | NA | NULL | NA | 140 | 42.42470 | -71.18468 | NA | NA | 1 | 7922 | STOPPED_AT | 1759967988 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759968000 | 9 | 10 | 2025 |
| y2112 | 71694942 | 77 | 1 | 19:31:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2112 | 2112 | NA | NULL | NA | 149 | 42.38921 | -71.11996 | NA | NA | 29 | 12301 | IN_TRANSIT_TO | 1759967987 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759968000 | 9 | 10 | 2025 |
| y3152 | 71694901 | 77 | 1 | 19:46:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y3152 | 3152 | NA | NULL | NA | 137 | 42.40118 | -71.13667 | NA | NA | 20 | 22671 | STOPPED_AT | 1759967992 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759968000 | 9 | 10 | 2025 |
| y1972 | 71694978 | 77 | 0 | 19:54:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y1972 | 1972 | NA | NULL | NA | 0 | 42.38331 | -71.11950 | NA | NA | 4 | 2314 | IN_TRANSIT_TO | 1759967995 | NA | MANY_SEATS_AVAILABLE | 40 | NULL | 1759968000 | 9 | 10 | 2025 |
| y1983 | 71694896 | 77 | 0 | 19:30:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y1983 | 1983 | NA | NULL | NA | 101 | 42.41516 | -71.15166 | NA | NA | 22 | 2282 | STOPPED_AT | 1759967995 | NA | FEW_SEATS_AVAILABLE | 40 | NULL | 1759968000 | 9 | 10 | 2025 |
| y1983 | 71694896 | 77 | 0 | 19:30:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y1983 | 1983 | NA | NULL | NA | 101 | 42.41515 | -71.15163 | NA | NA | 22 | 2282 | STOPPED_AT | 1759967997 | NA | FEW_SEATS_AVAILABLE | 40 | NULL | 1759968003 | 9 | 10 | 2025 |
| y2109 | 71695028 | 77 | 0 | 19:42:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2109 | 2109 | NA | NULL | NA | 310 | 42.39970 | -71.13308 | NA | NA | 12 | 2273 | STOPPED_AT | 1759967995 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759968003 | 9 | 10 | 2025 |
| y2109 | 71695028 | 77 | 0 | 19:42:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2109 | 2109 | NA | NULL | NA | 310 | 42.39970 | -71.13308 | NA | NA | 12 | 2273 | IN_TRANSIT_TO | 1759967995 | NA | MANY_SEATS_AVAILABLE | 0 | NULL | 1759968000 | 9 | 10 | 2025 |
| y2112 | 71694942 | 77 | 1 | 19:31:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2112 | 2112 | NA | NULL | NA | 158 | 42.38904 | -71.11986 | NA | NA | 29 | 12301 | IN_TRANSIT_TO | 1759967998 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759968001 | 9 | 10 | 2025 |
| y2112 | 71694942 | 77 | 1 | 19:31:00 | 20251008 | SCHEDULED | NA | NA | NA | NA | TRUE | FALSE | y2112 | 2112 | NA | NULL | NA | 180 | 42.38836 | -71.11955 | NA | NA | 29 | 12301 | STOPPED_AT | 1759968040 | NA | MANY_SEATS_AVAILABLE | 20 | NULL | 1759968042 | 9 | 10 | 2025 |

Displaying records 1 - 10

`lamp.read_date_partitioned` can accept a list of dates (as above) or
start and end dates:

``` sql
SELECT *
FROM lamp.read_date_partitioned(
    "RT_VEHICLE_POSITIONS",
    DATE '2025-10-01',
    DATE '2025-10-09'
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
