# Why are trips missing from LRTP data?
crunkel@mbta.com
2025-08-20

- [The Problem](#the-problem)
  - [Replicating the problem](#replicating-the-problem)
- [Where are the missing trips?](#where-are-the-missing-trips)
- [A different way of connecting
  trips](#a-different-way-of-connecting-trips)
- [tl;dr](#tldr)

# The Problem

Crissy
[reported](https://app.asana.com/1/15492006741476/project/1189492770004753/task/1211080535383270?focus=true)
that LAMP Tableau data was missing trips that were available in
Performance Analyzer. Where are those trips and why aren’t they visible
Performance Analyzer?

<details class="code-fold">
<summary>Show the code</summary>

``` python
from datetime import date
import polars as pl
from lamp_py.aws import s3
from lamp_py.runtime_utils import remote_files as rf

pl.Config(tbl_rows=25)

service_date = date(2025, 8, 6)
```

</details>

## Replicating the problem

Firas shared 89 trips from August 6, 2025 along the B line that were not
matched by the data LAMP published in Tableau.

<details open class="code-fold">
<summary>Show the code</summary>

``` python
unmatched = pl.read_csv("~/Downloads/unmatched_clean.csv")
unmatched.glimpse()
```

</details>

    Rows: 89
    Columns: 5
    $ trip_id           <i64> 69329457, 69329950, 69329956, 69329958, 69329960, 69329962, 69329964, 69329966, 69329968, 69329970
    $ consist           <str> '3845-3633', '3805-3647', '3706-3830', '3634-3817', '3706-3830', '3830', '3922', '3809-3696', '3634-3817', '3634-3817'
    $ PA_departure_time <str> '2025-08-06 14:53:03 EDT', '2025-08-06 06:29:36 EDT', '2025-08-06 08:39:04 EDT', '2025-08-06 10:48:07 EDT', '2025-08-06 12:46:22 EDT', '2025-08-06 13:56:27 EDT', '2025-08-06 16:20:48 EDT', '2025-08-06 18:40:21 EDT', '2025-08-06 07:04:49 EDT', '2025-08-06 08:46:52 EDT'
    $ TB_departure_time <str> None, '2025-08-06 06:39:02 EDT', '2025-08-06 08:46:52 EDT', '2025-08-06 10:40:56 EDT', '2025-08-06 12:46:23 EDT', '2025-08-06 14:29:27 EDT', '2025-08-06 16:30:33 EDT', None, '2025-08-06 07:04:50 EDT', '2025-08-06 08:55:19 EDT'
    $ mismatch_reason   <str> 'No matching trip_id in Tableau', 'Departure time difference exceeds tolerance', 'Departure time difference exceeds tolerance', 'Departure time difference exceeds tolerance', 'Generated time difference exceeds tolerance; Predicted departure time difference exceeds tolerance', 'Departure time difference exceeds tolerance', 'Departure time difference exceeds tolerance', 'No matching trip_id in Tableau', 'Generated time difference exceeds tolerance', 'Departure time difference exceeds tolerance'

Trips were matched by:

> trip_ID, prediction generation time, predicted departure time, and
> actual departure time – going one-way from Prediction Analyzer to
> Tableau (since Prediction Analyzer has a lower polling rate for
> predictions, we have much less data for any given service day there
> vs. Tableau);
>
> - I ignored the consists for the sake of analysis for now (and we’re
>   not focusing too much on predicted consist at this point in time);
> - I allowed for a 5-second tolerance for actual departure time as we
>   expect slight differences between Prediction Analyzer and Tableau
>   for this field.
> - I allowed for a 60-second tolerance for prediction generation time
>   and predicted time as we should at least find a Tableau record that
>   matches to within a minute of every Prediction Analyzer record.

Any unmatched trip must violate one of these conditions. To start
investigating this issue, I’ll filter the unmatched dataset down to
trips that are listed as totally missing, resulting in 25 trips.

<details class="code-fold">
<summary>Show the code</summary>

``` python
missing = (
    unmatched
    .filter(pl.col("TB_departure_time").is_null())
    .with_columns(pl.col("trip_id").cast(str).alias("trip_id"))
)
missing.glimpse()
```

</details>

    Rows: 25
    Columns: 5
    $ trip_id           <str> '69329457', '69329966', '69329986', '69329992', '69330000', '69330008', '69330036', '69330040', '69330048', '69330056'
    $ consist           <str> '3845-3633', '3809-3696', '3917', '3845-3633', '3821-3682', '3849-3673', '3914-3917', '3921', '3821-3682', '3861-3659'
    $ PA_departure_time <str> '2025-08-06 14:53:03 EDT', '2025-08-06 18:40:21 EDT', '2025-08-06 11:06:25 EDT', '2025-08-06 16:42:31 EDT', '2025-08-06 11:13:09 EDT', '2025-08-06 19:02:45 EDT', '2025-08-06 19:23:04 EDT', '2025-08-06 09:27:06 EDT', '2025-08-06 17:20:37 EDT', '2025-08-06 11:39:39 EDT'
    $ TB_departure_time <str> None, None, None, None, None, None, None, None, None, None
    $ mismatch_reason   <str> 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau', 'No matching trip_id in Tableau'

Starting with missing trips will focus the problem a bit, because it’s
easier to check for something that is misisng entirely. Now, let’s
double-check that we see the same thing, that these trip IDs are not
present in LAMP’s Tableau data for August 6.

<details class="code-fold">
<summary>Show the code</summary>

``` python
tableau = pl.read_parquet("s3://mbta-performance/lamp/tableau/gtfs-rt/LAMP_RT_VehiclePositions_LR_60_day.parquet")
```

</details>

We can do so by joining the Tableau parquet file to these missing trips.
If this join returns no rows, then all the trips are missing in the
Tableau data for August 6.

<details class="code-fold">
<summary>Show the code</summary>

``` python
(
    missing
    .join(
        tableau
        .filter(pl.col("vehicle.trip.start_date") == "20250806")
        .group_by("vehicle.trip.trip_id", "vehicle.trip.start_time")
        .agg(pl.min("vehicle.timestamp")),
        right_on = "vehicle.trip.trip_id",
        left_on = "trip_id",
        how = "inner"
    )
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
<small>shape: (1, 7)</small><table border="1" class="dataframe"><thead><tr><th>trip_id</th><th>consist</th><th>PA_departure_time</th><th>TB_departure_time</th><th>mismatch_reason</th><th>vehicle.trip.start_time</th><th>vehicle.timestamp</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>datetime[μs]</td></tr></thead><tbody><tr><td>&quot;69329457&quot;</td><td>&quot;3845-3633&quot;</td><td>&quot;2025-08-06 14:53:03 EDT&quot;</td><td>null</td><td>&quot;No matching trip_id in Tableau&quot;</td><td>&quot;14:38:00&quot;</td><td>2025-08-06 14:39:40</td></tr></tbody></table></div>

Interesting—there is one trip ID among the Tableau data. Let’s look more
closely at this trip:

<details class="code-fold">
<summary>Show the code</summary>

``` python
(
    tableau
    .filter(
        pl.col("vehicle.trip.trip_id") == "69329457",
        pl.col("vehicle.trip.start_date") == "20250806"
    )
    .group_by(
        "vehicle.trip.trip_id",
        "vehicle.trip.route_id",
        "vehicle.vehicle.label",
        "vehicle.trip.start_time",
    )
    .agg(
        pl.first("vehicle.timestamp").alias("earliest_timestamp"),
        pl.first("vehicle.stop_id").alias("first_stop"),
    )
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
<small>shape: (2, 6)</small><table border="1" class="dataframe"><thead><tr><th>vehicle.trip.trip_id</th><th>vehicle.trip.route_id</th><th>vehicle.vehicle.label</th><th>vehicle.trip.start_time</th><th>earliest_timestamp</th><th>first_stop</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>datetime[μs]</td><td>str</td></tr></thead><tbody><tr><td>&quot;69329457&quot;</td><td>&quot;Green-D&quot;</td><td>&quot;3859-3713&quot;</td><td>&quot;14:38:00&quot;</td><td>2025-08-06 14:40:13</td><td>&quot;70162&quot;</td></tr><tr><td>&quot;69329457&quot;</td><td>&quot;Green-D&quot;</td><td>&quot;3713-3859&quot;</td><td>&quot;14:38:00&quot;</td><td>2025-08-06 14:39:40</td><td>&quot;70162&quot;</td></tr></tbody></table></div>

Okay, so Tableau has this trip ID but:

1.  on the D line (at Woodland)
2.  with a different consist than expected[^1]

# Where are the missing trips?

As for the other trip IDs, it’s worth noting that some *do* exist for
other service dates:

<details class="code-fold">
<summary>Show the code</summary>

``` python
(
    missing
    .join(
        tableau.select("vehicle.trip.trip_id", "vehicle.trip.start_date"),
        right_on = "vehicle.trip.trip_id",
        left_on = "trip_id",
        how = "inner"
    )
    .select("trip_id", "vehicle.trip.start_date")
    .group_by(pl.col("vehicle.trip.start_date").str.to_date("%Y%m%d"))
    .n_unique()
    .sort("vehicle.trip.start_date")
    .rename({
        "vehicle.trip.start_date": "Service Date",
        "trip_id": "Count of Missing Trip IDs Present in GFTS-RT"
    })
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
<small>shape: (43, 2)</small><table border="1" class="dataframe"><thead><tr><th>Service Date</th><th>Count of Missing Trip IDs Present in GFTS-RT</th></tr><tr><td>date</td><td>u32</td></tr></thead><tbody><tr><td>2025-06-20</td><td>6</td></tr><tr><td>2025-06-23</td><td>10</td></tr><tr><td>2025-06-24</td><td>14</td></tr><tr><td>2025-06-25</td><td>10</td></tr><tr><td>2025-06-26</td><td>11</td></tr><tr><td>2025-06-27</td><td>19</td></tr><tr><td>2025-06-30</td><td>12</td></tr><tr><td>2025-07-01</td><td>14</td></tr><tr><td>2025-07-02</td><td>20</td></tr><tr><td>2025-07-03</td><td>19</td></tr><tr><td>2025-07-07</td><td>12</td></tr><tr><td>2025-07-08</td><td>20</td></tr><tr><td>2025-07-09</td><td>17</td></tr><tr><td>&hellip;</td><td>&hellip;</td></tr><tr><td>2025-08-05</td><td>18</td></tr><tr><td>2025-08-06</td><td>1</td></tr><tr><td>2025-08-07</td><td>14</td></tr><tr><td>2025-08-08</td><td>18</td></tr><tr><td>2025-08-11</td><td>8</td></tr><tr><td>2025-08-12</td><td>12</td></tr><tr><td>2025-08-13</td><td>10</td></tr><tr><td>2025-08-14</td><td>18</td></tr><tr><td>2025-08-15</td><td>17</td></tr><tr><td>2025-08-18</td><td>7</td></tr><tr><td>2025-08-19</td><td>18</td></tr><tr><td>2025-08-20</td><td>3</td></tr></tbody></table></div>

Can the GTFS Schedule tell us more about these trip IDs? If they appear
on other service dates, chances are they’re on our schedule. To check
this, we first need the GTFS Schedule tables for trips and stop times.

<details class="code-fold">
<summary>Show the code</summary>

``` python
trips = (
    pl.scan_parquet("s3://mbta-performance/lamp/gtfs_archive/2025/stop_times.parquet")
    .filter(pl.col("stop_sequence") == 1)
    .join(
        pl.scan_parquet("s3://mbta-performance/lamp/gtfs_archive/2025/trips.parquet"),
        on = "trip_id",
        how = "inner"
    )
    .select("trip_id", "departure_time", "stop_id")
    .collect()
)
```

</details>

I’ve pulled in the first `stop_id` and `departure_time` for each
scheduled trip so that we can compare those to Prediction Analyzer’s
departure times from Boston College (`70106`).

<details class="code-fold">
<summary>Show the code</summary>

``` python
(
    missing
    .select("trip_id", "consist", "PA_departure_time")
    .join(
        trips.rename({
            "departure_time": "Scheduled departure time",
            "stop_id": "Scheduled terminal"
        }),
        on = "trip_id",
        how = "left"
    )
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
<small>shape: (25, 5)</small><table border="1" class="dataframe"><thead><tr><th>trip_id</th><th>consist</th><th>PA_departure_time</th><th>Scheduled departure time</th><th>Scheduled terminal</th></tr><tr><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;69329457&quot;</td><td>&quot;3845-3633&quot;</td><td>&quot;2025-08-06 14:53:03 EDT&quot;</td><td>null</td><td>null</td></tr><tr><td>&quot;69329966&quot;</td><td>&quot;3809-3696&quot;</td><td>&quot;2025-08-06 18:40:21 EDT&quot;</td><td>&quot;18:26:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69329986&quot;</td><td>&quot;3917&quot;</td><td>&quot;2025-08-06 11:06:25 EDT&quot;</td><td>&quot;10:56:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69329992&quot;</td><td>&quot;3845-3633&quot;</td><td>&quot;2025-08-06 16:42:31 EDT&quot;</td><td>&quot;16:45:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330000&quot;</td><td>&quot;3821-3682&quot;</td><td>&quot;2025-08-06 11:13:09 EDT&quot;</td><td>&quot;11:05:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330008&quot;</td><td>&quot;3849-3673&quot;</td><td>&quot;2025-08-06 19:02:45 EDT&quot;</td><td>&quot;18:55:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330036&quot;</td><td>&quot;3914-3917&quot;</td><td>&quot;2025-08-06 19:23:04 EDT&quot;</td><td>&quot;19:13:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330040&quot;</td><td>&quot;3921&quot;</td><td>&quot;2025-08-06 09:27:06 EDT&quot;</td><td>&quot;09:35:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330048&quot;</td><td>&quot;3821-3682&quot;</td><td>&quot;2025-08-06 17:20:37 EDT&quot;</td><td>&quot;17:20:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330056&quot;</td><td>&quot;3861-3659&quot;</td><td>&quot;2025-08-06 11:39:39 EDT&quot;</td><td>&quot;11:38:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330062&quot;</td><td>&quot;3717-3880&quot;</td><td>&quot;2025-08-06 17:28:41 EDT&quot;</td><td>&quot;17:28:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330064&quot;</td><td>&quot;3717-3880&quot;</td><td>&quot;2025-08-06 19:38:03 EDT&quot;</td><td>&quot;19:29:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330068&quot;</td><td>&quot;3861-3659&quot;</td><td>&quot;2025-08-06 09:44:35 EDT&quot;</td><td>&quot;09:51:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330080&quot;</td><td>&quot;3805-3647&quot;</td><td>&quot;2025-08-06 10:04:34 EDT&quot;</td><td>&quot;09:59:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330090&quot;</td><td>&quot;3805-3647&quot;</td><td>&quot;2025-08-06 08:06:55 EDT&quot;</td><td>&quot;08:15:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330112&quot;</td><td>&quot;3604-3871&quot;</td><td>null</td><td>&quot;18:00:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330124&quot;</td><td>&quot;3805-3647&quot;</td><td>&quot;2025-08-06 18:20:35 EDT&quot;</td><td>&quot;18:09:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330132&quot;</td><td>&quot;3849-3673&quot;</td><td>&quot;2025-08-06 14:54:46 EDT&quot;</td><td>&quot;14:21:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330138&quot;</td><td>&quot;3919-3915&quot;</td><td>&quot;2025-08-06 19:48:05 EDT&quot;</td><td>&quot;19:38:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330142&quot;</td><td>&quot;3921&quot;</td><td>&quot;2025-08-06 20:04:50 EDT&quot;</td><td>&quot;19:56:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330148&quot;</td><td>&quot;3805-3647&quot;</td><td>&quot;2025-08-06 20:21:25 EDT&quot;</td><td>&quot;20:22:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330152&quot;</td><td>&quot;3809-3696&quot;</td><td>&quot;2025-08-06 20:34:40 EDT&quot;</td><td>&quot;20:41:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330162&quot;</td><td>&quot;3821-3682&quot;</td><td>&quot;2025-08-06 21:23:07 EDT&quot;</td><td>&quot;21:27:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330166&quot;</td><td>&quot;3919-3915&quot;</td><td>&quot;2025-08-06 21:46:25 EDT&quot;</td><td>&quot;21:45:00&quot;</td><td>&quot;70106&quot;</td></tr><tr><td>&quot;69330172&quot;</td><td>&quot;3604-3871&quot;</td><td>&quot;2025-08-06 22:23:06 EDT&quot;</td><td>&quot;22:13:00&quot;</td><td>&quot;70106&quot;</td></tr></tbody></table></div>

I see 2 things:

1.  The only trip present in the Prediction Analyzer dataset that has no
    match in our schedule is the D-line trip that Tableau reported. This
    begs the question **how does an unscheduled trip get a trip ID in
    Prediction Analyzer and a different trip ID in Tableau?** Let’s
    check which trip ID we received from GTFS-RT for that
    possibly-D-line, possibly-B-line trip. To check, we download the
    zipped JSON archived at 2025-08-06 14:39:40 EDT, when we supposedly
    saw trip `69329457` and explode it to a dataframe.

<details class="code-fold">
<summary>Show the code</summary>

``` python
incoming_vehicle_position = (
    pl.read_ndjson("s3://mbta-ctd-dataplatform-dev-archive/lamp/delta/2025/08/06/2025-08-06T18:40:13Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz") # UTC timestamp is 4 hours ahead
    .select("entity")
    .explode("entity")
    .unnest("entity")
    .unnest("vehicle")
    .unnest("trip")
)
```

</details>

Then, we filter to the Woodland stop:

<details class="code-fold">
<summary>Show the code</summary>

``` python
incoming_vehicle_position.filter(pl.col("stop_id") == "70162")
```

</details>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (1, 18)</small><table border="1" class="dataframe"><thead><tr><th>id</th><th>current_status</th><th>current_stop_sequence</th><th>multi_carriage_details</th><th>position</th><th>stop_id</th><th>timestamp</th><th>start_time</th><th>trip_id</th><th>route_id</th><th>direction_id</th><th>start_date</th><th>schedule_relationship</th><th>last_trip</th><th>revenue</th><th>vehicle</th><th>occupancy_percentage</th><th>occupancy_status</th></tr><tr><td>str</td><td>str</td><td>i64</td><td>list[struct[5]]</td><td>struct[4]</td><td>str</td><td>i64</td><td>str</td><td>str</td><td>str</td><td>i64</td><td>str</td><td>str</td><td>bool</td><td>bool</td><td>struct[2]</td><td>i64</td><td>str</td></tr></thead><tbody><tr><td>&quot;G-10238&quot;</td><td>&quot;INCOMING_AT&quot;</td><td>320</td><td>[{&quot;3859&quot;,&quot;NO_DATA_AVAILABLE&quot;,&quot;AB&quot;,1,null}, {&quot;3713&quot;,&quot;NO_DATA_AVAILABLE&quot;,&quot;BA&quot;,2,null}]</td><td>{135,42.33443,-71.24654,17.0}</td><td>&quot;70162&quot;</td><td>1754505609</td><td>&quot;14:38:00&quot;</td><td>&quot;69329457&quot;</td><td>&quot;Green-D&quot;</td><td>1</td><td>&quot;20250806&quot;</td><td>&quot;SCHEDULED&quot;</td><td>false</td><td>true</td><td>{&quot;G-10238&quot;,&quot;3859-3713&quot;}</td><td>null</td><td>null</td></tr></tbody></table></div>

The `trip_id` is `69329457`—it didn’t change between ingestion and
publication to Tableau—so, from now on, I’ll refer to the Tableau trip
data as GTFS-RT data.

2.  The scheduled trip times are off of the departure times reported by
    Prediction Analyzer by several minutes but are somewhat close to the
    scheduled trip times. **Can we use the times to re-connect
    Prediction Analyzer to a GTFS-RT?**

# A different way of connecting trips

Instead of connecting trips in Prediction Analyzer to GFTS-RT by ID, can
we connect trips using time? Let’s start with one trip, the scheduled
D-line trip that appeared on the B-line. We’ll connect the trip by using
the nearest LTRP timestamp on the B-line.

<details class="code-fold">
<summary>Show the code</summary>

``` python
(
    missing
    .filter(pl.col("trip_id") == "69329457")
    .with_columns(pl.col("PA_departure_time").str.to_datetime("%Y-%m-%d %H:%M:%S %Z"))
    .join_asof(
        tableau
        .filter(pl.col("vehicle.trip.route_id") == "Green-B")
        .filter(pl.col("vehicle.trip.start_date") == "20250806")
        .sort("vehicle.timestamp"),
        left_on = "PA_departure_time",
        right_on = "vehicle.timestamp",
        strategy = "nearest"
    )
    .select(
        "PA_departure_time",
        "vehicle.timestamp",
        "trip_id",
        "vehicle.trip.trip_id",
        "vehicle.trip.route_id",
        "consist",
        "vehicle.vehicle.label"
    )
    .rename({
        "consist": "Consist",
        "PA_departure_time": "PA Departure",
        "trip_id": "PA Trip ID",
        "vehicle.trip.trip_id": "GTFS Trip ID",
        "vehicle.timestamp": "GTFS Timestamp",
        "vehicle.trip.route_id": "GTFS Route"
    })
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
<small>shape: (1, 7)</small><table border="1" class="dataframe"><thead><tr><th>PA Departure</th><th>GTFS Timestamp</th><th>PA Trip ID</th><th>GTFS Trip ID</th><th>GTFS Route</th><th>Consist</th><th>vehicle.vehicle.label</th></tr><tr><td>datetime[μs]</td><td>datetime[μs]</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>2025-08-06 14:53:03</td><td>2025-08-06 14:53:03</td><td>&quot;69329457&quot;</td><td>&quot;69329990&quot;</td><td>&quot;Green-B&quot;</td><td>&quot;3845-3633&quot;</td><td>&quot;3845-3633&quot;</td></tr></tbody></table></div>

Woah, this strategy returns an exact time match. What’s more, it also
returns an exact consist match. **Can we use consist and time but ignore
`trip_id` to find the unmatched trips?**

To test this, I’ll join all the trips Firas listed as unmatched to the
trips in our Tableau data but, instead of joining on trip ID, I’ll join
on

- consist
- stop (in this case, the second-to-last stop on the B-line
  `South Street`)
- closest time point

<details class="code-fold">
<summary>Show the code</summary>

``` python
consist_time_join = (
    unmatched
    .filter(pl.col("PA_departure_time").is_not_null())
    .with_columns(pl.col("PA_departure_time").str.to_datetime("%Y-%m-%d %H:%M:%S %Z"))
    .sort("PA_departure_time")
    .join_asof(
        tableau
        .filter(pl.col("vehicle.trip.route_id") == "Green-B")
        .filter(pl.col("vehicle.trip.start_date") == "20250806")
        .sort("vehicle.timestamp"),
        left_on = "PA_departure_time",
        right_on = "vehicle.timestamp",
        by_left = "consist",
        by_right = "vehicle.vehicle.label",
        strategy = "nearest"
    )
    .select(
        "consist",
        "PA_departure_time",
        "vehicle.timestamp",
        "trip_id",
        "vehicle.trip.trip_id",
        "vehicle.trip.route_id"
    )
    .rename({
        "consist": "Consist",
        "PA_departure_time": "PA Departure",
        "trip_id": "PA Trip ID",
        "vehicle.trip.trip_id": "GTFS Trip ID",
        "vehicle.timestamp": "GTFS Timestamp",
        "vehicle.trip.route_id": "GTFS Route"
    })
)
```

</details>

Doing so results in an exact match for each trip with a listed departure
time in Prediction Analyzer:

<details class="code-fold">
<summary>Show the code</summary>

``` python
consist_time_join
```

</details>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (80, 6)</small><table border="1" class="dataframe"><thead><tr><th>Consist</th><th>PA Departure</th><th>GTFS Timestamp</th><th>PA Trip ID</th><th>GTFS Trip ID</th><th>GTFS Route</th></tr><tr><td>str</td><td>datetime[μs]</td><td>datetime[μs]</td><td>i64</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;3805-3647&quot;</td><td>2025-08-06 06:29:36</td><td>2025-08-06 06:29:36</td><td>69329950</td><td>&quot;69329928&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3634-3817&quot;</td><td>2025-08-06 07:04:49</td><td>2025-08-06 07:04:49</td><td>69329968</td><td>&quot;69329968&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3695-3844&quot;</td><td>2025-08-06 07:10:54</td><td>2025-08-06 07:10:54</td><td>69329996</td><td>&quot;69329982&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3914-3917&quot;</td><td>2025-08-06 07:17:06</td><td>2025-08-06 07:17:06</td><td>69330010</td><td>&quot;69329936&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3821-3682&quot;</td><td>2025-08-06 07:28:55</td><td>2025-08-06 07:28:55</td><td>69330024</td><td>&quot;69329939&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3717-3880&quot;</td><td>2025-08-06 07:34:55</td><td>2025-08-06 07:34:55</td><td>69330052</td><td>&quot;69330024&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3905-3921&quot;</td><td>2025-08-06 07:45:01</td><td>2025-08-06 07:45:01</td><td>69330066</td><td>&quot;69330038&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3805-3647&quot;</td><td>2025-08-06 08:06:55</td><td>2025-08-06 08:06:55</td><td>69330090</td><td>&quot;69330078&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3922&quot;</td><td>2025-08-06 08:15:05</td><td>2025-08-06 08:15:05</td><td>69330102</td><td>&quot;ADDED-1582982918&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3809-3696&quot;</td><td>2025-08-06 08:26:52</td><td>2025-08-06 08:26:52</td><td>69330114</td><td>&quot;69329951&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3706-3830&quot;</td><td>2025-08-06 08:39:04</td><td>2025-08-06 08:39:04</td><td>69329956</td><td>&quot;69329954&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3634-3817&quot;</td><td>2025-08-06 08:46:52</td><td>2025-08-06 08:46:52</td><td>69329970</td><td>&quot;69329956&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3914-3917&quot;</td><td>2025-08-06 09:02:50</td><td>2025-08-06 09:02:50</td><td>69329998</td><td>&quot;69329984&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>&quot;3914-3917&quot;</td><td>2025-08-06 21:13:11</td><td>2025-08-06 21:13:11</td><td>69330160</td><td>&quot;69330158&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3821-3682&quot;</td><td>2025-08-06 21:23:07</td><td>2025-08-06 21:23:07</td><td>69330162</td><td>&quot;69330160&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3919-3915&quot;</td><td>2025-08-06 21:46:25</td><td>2025-08-06 21:46:25</td><td>69330166</td><td>&quot;ADDED-1582984672&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3861-3659&quot;</td><td>2025-08-06 21:59:40</td><td>2025-08-06 21:59:40</td><td>69330168</td><td>&quot;69330168&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3604-3871&quot;</td><td>2025-08-06 22:23:06</td><td>2025-08-06 22:23:06</td><td>69330172</td><td>&quot;69330174&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3805-3647&quot;</td><td>2025-08-06 22:31:38</td><td>2025-08-06 22:31:38</td><td>69330174</td><td>&quot;69330176&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3845-3633&quot;</td><td>2025-08-06 22:34:45</td><td>2025-08-06 22:59:42</td><td>69330178</td><td>&quot;ADDED-1582984792&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3809-3696&quot;</td><td>2025-08-06 22:41:25</td><td>2025-08-06 22:41:25</td><td>69330176</td><td>&quot;69330178&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3852-3634&quot;</td><td>2025-08-06 22:50:03</td><td>2025-08-06 22:50:03</td><td>69330182</td><td>&quot;69330157&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3821-3682&quot;</td><td>2025-08-06 23:19:31</td><td>2025-08-06 23:19:31</td><td>69330184</td><td>&quot;69330186&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3717-3880&quot;</td><td>2025-08-06 23:31:34</td><td>2025-08-06 23:31:34</td><td>69330188</td><td>&quot;69330188&quot;</td><td>&quot;Green-B&quot;</td></tr><tr><td>&quot;3919-3915&quot;</td><td>2025-08-06 23:43:00</td><td>2025-08-06 23:43:00</td><td>69330190</td><td>&quot;69330190&quot;</td><td>&quot;Green-B&quot;</td></tr></tbody></table></div>

None of the trip IDs match but the right car is at the right stop at the
right time.

# tl;dr

Prediction Analyzer and GTFS-RT agree on which Green Line trains are
running on a track but do not agree on which trip IDs to assign those
cars. Why do trip IDs not match between the 2 systems?

[^1]: We also see that this trip has 2 rows with the consist flipped.
    Why? Did a pullout inspector assign the trip and then change the
    consist from the last trip?
