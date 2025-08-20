# Why are trips missing from LRTP data?


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
present in LAMP data for August 6.

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
<small>shape: (1, 7)</small>

| trip_id | consist | PA_departure_time | TB_departure_time | mismatch_reason | vehicle.trip.start_time | vehicle.timestamp |
|----|----|----|----|----|----|----|
| str | str | str | str | str | str | datetime\[μs\] |
| "69329457" | "3845-3633" | "2025-08-06 14:53:03 EDT" | null | "No matching trip_id in Tableau" | "14:38:00" | 2025-08-06 14:39:40 |

</div>

Interesting—there is one trip ID among the GTFS data. Let’s look more
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
<small>shape: (2, 6)</small>

| vehicle.trip.trip_id | vehicle.trip.route_id | vehicle.vehicle.label | vehicle.trip.start_time | earliest_timestamp | first_stop |
|----|----|----|----|----|----|
| str | str | str | str | datetime\[μs\] | str |
| "69329457" | "Green-D" | "3859-3713" | "14:38:00" | 2025-08-06 14:40:13 | "70162" |
| "69329457" | "Green-D" | "3713-3859" | "14:38:00" | 2025-08-06 14:39:40 | "70162" |

</div>

Okay, so GTFS-RT has this trip ID but:

1.  on the D line
2.  with a different consist than expected

<div class="column-margin">

We also see that this trip has 2 rows with the consist flipped. Why? Did
a pullout inspector assign the trip and then change the consist from the
last trip?

</div>

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
<small>shape: (43, 2)</small>

| Service Date | Count of Missing Trip IDs Present in GFTS-RT |
|--------------|----------------------------------------------|
| date         | u32                                          |
| 2025-06-20   | 6                                            |
| 2025-06-23   | 10                                           |
| 2025-06-24   | 14                                           |
| 2025-06-25   | 10                                           |
| 2025-06-26   | 11                                           |
| 2025-06-27   | 19                                           |
| 2025-06-30   | 12                                           |
| 2025-07-01   | 14                                           |
| 2025-07-02   | 20                                           |
| 2025-07-03   | 19                                           |
| 2025-07-07   | 12                                           |
| 2025-07-08   | 20                                           |
| 2025-07-09   | 17                                           |
| …            | …                                            |
| 2025-08-05   | 18                                           |
| 2025-08-06   | 1                                            |
| 2025-08-07   | 14                                           |
| 2025-08-08   | 18                                           |
| 2025-08-11   | 8                                            |
| 2025-08-12   | 12                                           |
| 2025-08-13   | 10                                           |
| 2025-08-14   | 18                                           |
| 2025-08-15   | 17                                           |
| 2025-08-18   | 7                                            |
| 2025-08-19   | 18                                           |
| 2025-08-20   | 2                                            |

</div>

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
<small>shape: (25, 5)</small>

| trip_id | consist | PA_departure_time | Scheduled departure time | Scheduled terminal |
|----|----|----|----|----|
| str | str | str | str | str |
| "69329457" | "3845-3633" | "2025-08-06 14:53:03 EDT" | null | null |
| "69329966" | "3809-3696" | "2025-08-06 18:40:21 EDT" | "18:26:00" | "70106" |
| "69329986" | "3917" | "2025-08-06 11:06:25 EDT" | "10:56:00" | "70106" |
| "69329992" | "3845-3633" | "2025-08-06 16:42:31 EDT" | "16:45:00" | "70106" |
| "69330000" | "3821-3682" | "2025-08-06 11:13:09 EDT" | "11:05:00" | "70106" |
| "69330008" | "3849-3673" | "2025-08-06 19:02:45 EDT" | "18:55:00" | "70106" |
| "69330036" | "3914-3917" | "2025-08-06 19:23:04 EDT" | "19:13:00" | "70106" |
| "69330040" | "3921" | "2025-08-06 09:27:06 EDT" | "09:35:00" | "70106" |
| "69330048" | "3821-3682" | "2025-08-06 17:20:37 EDT" | "17:20:00" | "70106" |
| "69330056" | "3861-3659" | "2025-08-06 11:39:39 EDT" | "11:38:00" | "70106" |
| "69330062" | "3717-3880" | "2025-08-06 17:28:41 EDT" | "17:28:00" | "70106" |
| "69330064" | "3717-3880" | "2025-08-06 19:38:03 EDT" | "19:29:00" | "70106" |
| "69330068" | "3861-3659" | "2025-08-06 09:44:35 EDT" | "09:51:00" | "70106" |
| "69330080" | "3805-3647" | "2025-08-06 10:04:34 EDT" | "09:59:00" | "70106" |
| "69330090" | "3805-3647" | "2025-08-06 08:06:55 EDT" | "08:15:00" | "70106" |
| "69330112" | "3604-3871" | null | "18:00:00" | "70106" |
| "69330124" | "3805-3647" | "2025-08-06 18:20:35 EDT" | "18:09:00" | "70106" |
| "69330132" | "3849-3673" | "2025-08-06 14:54:46 EDT" | "14:21:00" | "70106" |
| "69330138" | "3919-3915" | "2025-08-06 19:48:05 EDT" | "19:38:00" | "70106" |
| "69330142" | "3921" | "2025-08-06 20:04:50 EDT" | "19:56:00" | "70106" |
| "69330148" | "3805-3647" | "2025-08-06 20:21:25 EDT" | "20:22:00" | "70106" |
| "69330152" | "3809-3696" | "2025-08-06 20:34:40 EDT" | "20:41:00" | "70106" |
| "69330162" | "3821-3682" | "2025-08-06 21:23:07 EDT" | "21:27:00" | "70106" |
| "69330166" | "3919-3915" | "2025-08-06 21:46:25 EDT" | "21:45:00" | "70106" |
| "69330172" | "3604-3871" | "2025-08-06 22:23:06 EDT" | "22:13:00" | "70106" |

</div>

The first thing I see is that the only trip present in the Prediction
Analyzer dataset that has no match in our schedule is the D-line trip
that GTFS-RT reported. This begs the question **how does an unscheduled
trip get 1 trip ID in Prediction Analyzer and a different trip ID in
GTFS-RT?**

The second thing I see is that the scheduled trip times are off of the
departure times reported by Prediction Analyzer by several minutes but
are somewhat close to the scheduled trip times. **Can we use the times
to re-connect Prediction Analyzer to a GTFS-RT?**

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
<small>shape: (1, 7)</small>

| PA_departure_time | vehicle.timestamp | trip_id | vehicle.trip.trip_id | vehicle.trip.route_id | consist | vehicle.vehicle.label |
|----|----|----|----|----|----|----|
| datetime\[μs\] | datetime\[μs\] | str | str | str | str | str |
| 2025-08-06 14:53:03 | 2025-08-06 14:53:03 | "69329457" | "69329990" | "Green-B" | "3845-3633" | "3845-3633" |

</div>

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
<small>shape: (80, 6)</small>

| consist | PA_departure_time | vehicle.timestamp | trip_id | vehicle.trip.trip_id | vehicle.trip.route_id |
|----|----|----|----|----|----|
| str | datetime\[μs\] | datetime\[μs\] | i64 | str | str |
| "3805-3647" | 2025-08-06 06:29:36 | 2025-08-06 06:29:36 | 69329950 | "69329928" | "Green-B" |
| "3634-3817" | 2025-08-06 07:04:49 | 2025-08-06 07:04:49 | 69329968 | "69329968" | "Green-B" |
| "3695-3844" | 2025-08-06 07:10:54 | 2025-08-06 07:10:54 | 69329996 | "69329982" | "Green-B" |
| "3914-3917" | 2025-08-06 07:17:06 | 2025-08-06 07:17:06 | 69330010 | "69329936" | "Green-B" |
| "3821-3682" | 2025-08-06 07:28:55 | 2025-08-06 07:28:55 | 69330024 | "69329939" | "Green-B" |
| "3717-3880" | 2025-08-06 07:34:55 | 2025-08-06 07:34:55 | 69330052 | "69330024" | "Green-B" |
| "3905-3921" | 2025-08-06 07:45:01 | 2025-08-06 07:45:01 | 69330066 | "69330038" | "Green-B" |
| "3805-3647" | 2025-08-06 08:06:55 | 2025-08-06 08:06:55 | 69330090 | "69330078" | "Green-B" |
| "3922" | 2025-08-06 08:15:05 | 2025-08-06 08:15:05 | 69330102 | "ADDED-1582982918" | "Green-B" |
| "3809-3696" | 2025-08-06 08:26:52 | 2025-08-06 08:26:52 | 69330114 | "69329951" | "Green-B" |
| "3706-3830" | 2025-08-06 08:39:04 | 2025-08-06 08:39:04 | 69329956 | "69329954" | "Green-B" |
| "3634-3817" | 2025-08-06 08:46:52 | 2025-08-06 08:46:52 | 69329970 | "69329956" | "Green-B" |
| "3914-3917" | 2025-08-06 09:02:50 | 2025-08-06 09:02:50 | 69329998 | "69329984" | "Green-B" |
| … | … | … | … | … | … |
| "3914-3917" | 2025-08-06 21:13:11 | 2025-08-06 21:13:11 | 69330160 | "69330158" | "Green-B" |
| "3821-3682" | 2025-08-06 21:23:07 | 2025-08-06 21:23:07 | 69330162 | "69330160" | "Green-B" |
| "3919-3915" | 2025-08-06 21:46:25 | 2025-08-06 21:46:25 | 69330166 | "ADDED-1582984672" | "Green-B" |
| "3861-3659" | 2025-08-06 21:59:40 | 2025-08-06 21:59:40 | 69330168 | "69330168" | "Green-B" |
| "3604-3871" | 2025-08-06 22:23:06 | 2025-08-06 22:23:06 | 69330172 | "69330174" | "Green-B" |
| "3805-3647" | 2025-08-06 22:31:38 | 2025-08-06 22:31:38 | 69330174 | "69330176" | "Green-B" |
| "3845-3633" | 2025-08-06 22:34:45 | 2025-08-06 22:59:42 | 69330178 | "ADDED-1582984792" | "Green-B" |
| "3809-3696" | 2025-08-06 22:41:25 | 2025-08-06 22:41:25 | 69330176 | "69330178" | "Green-B" |
| "3852-3634" | 2025-08-06 22:50:03 | 2025-08-06 22:50:03 | 69330182 | "69330157" | "Green-B" |
| "3821-3682" | 2025-08-06 23:19:31 | 2025-08-06 23:19:31 | 69330184 | "69330186" | "Green-B" |
| "3717-3880" | 2025-08-06 23:31:34 | 2025-08-06 23:31:34 | 69330188 | "69330188" | "Green-B" |
| "3919-3915" | 2025-08-06 23:43:00 | 2025-08-06 23:43:00 | 69330190 | "69330190" | "Green-B" |

</div>

None of the trip IDs match but the right car is at the right stop at the
right time.

# tl;dr

Prediction Analyzer and GTFS-RT agree on which Green Line trains are
running on a track but do not agree on which trip IDs to assign those
cars. Why do trip IDs not match between the 2 systems?
