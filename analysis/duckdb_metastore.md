# What would it take to create a duckdb metastore?
crunkel@mbta.com
2025-09-26

- [Currently](#currently)
- [Proposal](#proposal)
- [Optimizing for speed](#optimizing-for-speed)
- [A 2-prong solution](#a-2-prong-solution)
- [Some next steps](#some-next-steps)

# Currently

DuckDB is a popular entrypoint for LAMP data. To access our s3 buckets,
users need to authenticate using their IAM account, which DuckDB
supports natively:

``` python
import duckdb
duckdb.sql("INSTALL aws")
duckdb.sql("LOAD aws")
duckdb.sql("""CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER credential_chain
)""")
duckdb.sql("SET unsafe_disable_etag_checks = true") # some of these cells take a loooonnnng time
```

And then query individual files using the `read_parquet` function:

``` python
duckdb.sql("""
    SELECT *
    FROM read_parquet('s3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/year=2025/month=9/day=26/2025-09-26T00:00:00.parquet')
    LIMIT 10
""")
```

    ┌─────────────────┬────────────┬──────────────────────────┬───────────────────────────┬────────────────────────────┬────────────────────────┬───────────────────────────┬─────────────────────────┬───────────────────┬──────────────────────┬───────────────────────┬───────────────────────────┬─────────────────────────┬─────────────────────────┬────────────────────────────────────┬───────────────────────────────┬─────────────────────────┬──────────────────────────┬──────────────────────────────┬──────────────────────┬────────────────────────┬────────────────────┬───────────────────────┬───────────────────────────────┬───────────────────────────┬───────────────────────────────────┬─────────────────────┬─────────────────────────────┬────────────────────────────┬───────────────────────┬─────────────────────────────┬──────────────────┬────────────────┬─────────────────┬───────────────────────────────┬─────────────────┬────────────────────────┬──────────────┬──────────────────┬──────────────────────────────┬──────────────────────────┬────────────────┬───────┬───────┬───────┐
    │       id        │ is_deleted │ vehicle.position.bearing │ vehicle.position.latitude │ vehicle.position.longitude │ vehicle.position.speed │ vehicle.position.odometer │ vehicle.location_source │ vehicle.timestamp │ vehicle.trip.trip_id │ vehicle.trip.route_id │ vehicle.trip.direction_id │ vehicle.trip.start_time │ vehicle.trip.start_date │ vehicle.trip.schedule_relationship │ vehicle.trip.route_pattern_id │ vehicle.trip.tm_trip_id │ vehicle.trip.overload_id │ vehicle.trip.overload_offset │ vehicle.trip.revenue │ vehicle.trip.last_trip │ vehicle.vehicle.id │ vehicle.vehicle.label │ vehicle.vehicle.license_plate │  vehicle.vehicle.consist  │ vehicle.vehicle.assignment_status │ vehicle.operator.id │ vehicle.operator.first_name │ vehicle.operator.last_name │ vehicle.operator.name │ vehicle.operator.logon_time │ vehicle.block_id │ vehicle.run_id │ vehicle.stop_id │ vehicle.current_stop_sequence │ vehicle.revenue │ vehicle.current_status │ vehicle.load │ vehicle.capacity │ vehicle.occupancy_percentage │ vehicle.occupancy_status │ feed_timestamp │  day  │ month │ year  │
    │     varchar     │  boolean   │          uint16          │          double           │           double           │         double         │          double           │         varchar         │      uint64       │       varchar        │        varchar        │           uint8           │         varchar         │         varchar         │              varchar               │            varchar            │         varchar         │          int64           │            int64             │       boolean        │        boolean         │      varchar       │        varchar        │            varchar            │ struct("label" varchar)[] │              varchar              │       varchar       │           varchar           │          varchar           │        varchar        │           uint64            │     varchar      │    varchar     │     varchar     │            uint32             │     boolean     │        varchar         │    uint16    │      uint16      │            uint16            │         varchar          │     uint64     │ int64 │ int64 │ int64 │
    ├─────────────────┼────────────┼──────────────────────────┼───────────────────────────┼────────────────────────────┼────────────────────────┼───────────────────────────┼─────────────────────────┼───────────────────┼──────────────────────┼───────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┼────────────────────────────────────┼───────────────────────────────┼─────────────────────────┼──────────────────────────┼──────────────────────────────┼──────────────────────┼────────────────────────┼────────────────────┼───────────────────────┼───────────────────────────────┼───────────────────────────┼───────────────────────────────────┼─────────────────────┼─────────────────────────────┼────────────────────────────┼───────────────────────┼─────────────────────────────┼──────────────────┼────────────────┼─────────────────┼───────────────────────────────┼─────────────────┼────────────────────────┼──────────────┼──────────────────┼──────────────────────────────┼──────────────────────────┼────────────────┼───────┼───────┼───────┤
    │ 1758844799_0802 │ false      │                      153 │                 42.237169 │                 -70.981176 │                 3.6097 │                      NULL │ samsara                 │        1758844799 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844801 │    26 │     9 │  2025 │
    │ 1758844812_0802 │ false      │                      123 │                 42.237147 │                 -70.981072 │                 4.7193 │                      NULL │ samsara                 │        1758844812 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844817 │    26 │     9 │  2025 │
    │ 1758844777_0808 │ false      │                      315 │                42.2519899 │                -71.0058437 │                   NULL │                      NULL │ transitmaster           │        1758844777 │ NULL                 │ 225                   │                      NULL │ NULL                    │ 20250925                │ SCHEDULED                          │ NULL                          │ 71987967                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0808              │ 0808                  │ NULL                          │ NULL                      │ NULL                              │ 535856              │ VICTOR                      │ MIREKU                     │ MIREKU                │                  1758834258 │ Q211-20          │ 128-1023       │ NULL            │                          NULL │ true            │ NULL                   │            3 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844800 │    26 │     9 │  2025 │
    │ 1758844816_0802 │ false      │                      149 │                 42.237031 │                  -70.98095 │                 0.5498 │                      NULL │ samsara                 │        1758844816 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844820 │    26 │     9 │  2025 │
    │ 1758844791_0802 │ false      │                      132 │                 42.237641 │                  -70.98173 │                13.0496 │                      NULL │ samsara                 │        1758844791 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844800 │    26 │     9 │  2025 │
    │ 1758844818_0802 │ false      │                      171 │                 42.237002 │                 -70.980947 │                    0.0 │                      NULL │ samsara                 │        1758844818 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844824 │    26 │     9 │  2025 │
    │ 1758844804_0802 │ false      │                      122 │                 42.237171 │                 -70.981168 │                    0.0 │                      NULL │ samsara                 │        1758844804 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844808 │    26 │     9 │  2025 │
    │ 1758844810_0802 │ false      │                      113 │                  42.23717 │                 -70.981113 │                 1.3795 │                      NULL │ samsara                 │        1758844810 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844814 │    26 │     9 │  2025 │
    │ 1758844825_0802 │ false      │                      131 │                 42.236884 │                 -70.980795 │                 7.4993 │                      NULL │ samsara                 │        1758844825 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844831 │    26 │     9 │  2025 │
    │ 1758844908_0802 │ false      │                       48 │                 42.231468 │                 -70.973437 │                    0.0 │                      NULL │ samsara                 │        1758844908 │ NULL                 │ 225                   │                      NULL │ NULL                    │ NULL                    │ SCHEDULED                          │ NULL                          │ 71987749                │                     NULL │                         NULL │ NULL                 │ NULL                   │ y0802              │ 0802                  │ NULL                          │ NULL                      │ NULL                              │ 532608              │ STIVE                       │ JIORDANY                   │ JIORDANY              │                  1758837764 │ Q225-67          │ 128-1044       │ NULL            │                          NULL │ true            │ NULL                   │            0 │               39 │                            0 │ MANY_SEATS_AVAILABLE     │     1758844910 │    26 │     9 │  2025 │
    ├─────────────────┴────────────┴──────────────────────────┴───────────────────────────┴────────────────────────────┴────────────────────────┴───────────────────────────┴─────────────────────────┴───────────────────┴──────────────────────┴───────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┴────────────────────────────────────┴───────────────────────────────┴─────────────────────────┴──────────────────────────┴──────────────────────────────┴──────────────────────┴────────────────────────┴────────────────────┴───────────────────────┴───────────────────────────────┴───────────────────────────┴───────────────────────────────────┴─────────────────────┴─────────────────────────────┴────────────────────────────┴───────────────────────┴─────────────────────────────┴──────────────────┴────────────────┴─────────────────┴───────────────────────────────┴─────────────────┴────────────────────────┴──────────────┴──────────────────┴──────────────────────────────┴──────────────────────────┴────────────────┴───────┴───────┴───────┤
    │ 10 rows                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    45 columns │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

This pattern is nearly as fast as querying data locally…

    'Query duration: 0.2321s'

but it’s not ergonomic to write. Querying records from multiple days
means subtly, predictably tweaking the URI. Further, querying multiple
records from multiple datasets—to join vehicle positions data to the
GTFS Schedule, for instance—asks users to also know the different
partitioning structures of each dataset.

# Proposal

Instead of treating our data as files, what if users could interact with
it as if it were tables in a database? For instance, the query above
would transform into:

``` python
duckdb.sql("""
    SELECT *
    FROM vehicle_positions
    WHERE file_date = '2025-09-26'
    LIMIT 10
""")
```

Fares already provides such a solution in the form of a duckdb
*metastore*. This metastore is technically a database instance but the
only data it holds is metadata mapping table names to s3 URIs. **The
result is queries that run as fast as if the URIs were typed out but are
written as a call to a database with a `WHERE` clause.**

DuckDB’s syntax makes creating metastores very simple, requiring users
only specify the pattern of the file partitioning. Here’s that
`vehicle_positions` table:

``` python
(
    duckdb
    .from_parquet("s3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/*/*/*/*.parquet", hive_partitioning = True)
    .select("*, make_date(year, month, day) AS file_date")
    .create_view("vehicle_positions", replace = True)
)
```

Now, we can crawl through the rest of the LAMP s3 buckets and catalog
each dataset as a table with a date key as the partition. Once we’ve
built a metastore of LAMP data, users can `ATTACH` their local DuckDB
instances to it and interact with LAMP as if it were in a relational
database but *without* the operational burden of serving a full
database.

But first, let’s check whether this works and runs nearly as
fast—listing files and indexing them would incur some overhead—as the
earlier query?

``` python
tick = perf_counter()
duckdb.sql("""
    SELECT *
    FROM vehicle_positions
    WHERE file_date = '2025-09-26'
    LIMIT 10
""")
f"Query duration: {(perf_counter() - tick):.4f}s"
```

    'Query duration: 25.7440s'

That’s unacceptably long. What happened? To find out, I asked the
database for its query plan:

``` python
print(duckdb.sql("""
    EXPLAIN ANALYZE
    SELECT min(file_date)
    FROM vehicle_positions
""").pl().row(0)[1])
```

    ┌─────────────────────────────────────┐
    │┌───────────────────────────────────┐│
    ││    Query Profiling Information    ││
    │└───────────────────────────────────┘│
    └─────────────────────────────────────┘
         EXPLAIN ANALYZE     SELECT min(file_date)     FROM vehicle_positions 
    ┌─────────────────────────────────────┐
    │┌───────────────────────────────────┐│
    ││         HTTPFS HTTP Stats         ││
    ││                                   ││
    ││            in: 0 bytes            ││
    ││            out: 0 bytes           ││
    ││              #HEAD: 0             ││
    ││              #GET: 0              ││
    ││              #PUT: 0              ││
    ││              #POST: 0             ││
    ││             #DELETE: 0            ││
    │└───────────────────────────────────┘│
    └─────────────────────────────────────┘
    ┌────────────────────────────────────────────────┐
    │┌──────────────────────────────────────────────┐│
    ││              Total Time: 28.96s              ││
    │└──────────────────────────────────────────────┘│
    └────────────────────────────────────────────────┘
    ┌───────────────────────────┐
    │           QUERY           │
    └─────────────┬─────────────┘
    ┌─────────────┴─────────────┐
    │      EXPLAIN_ANALYZE      │
    │    ────────────────────   │
    │           0 rows          │
    │          (0.00s)          │
    └─────────────┬─────────────┘
    ┌─────────────┴─────────────┐
    │    UNGROUPED_AGGREGATE    │
    │    ────────────────────   │
    │    Aggregates: min(#0)    │
    │                           │
    │           1 row           │
    │          (0.06s)          │
    └─────────────┬─────────────┘
    ┌─────────────┴─────────────┐
    │         PROJECTION        │
    │    ────────────────────   │
    │         file_date         │
    │                           │
    │     1,877,582,955 rows    │
    │          (0.04s)          │
    └─────────────┬─────────────┘
    ┌─────────────┴─────────────┐
    │         PROJECTION        │
    │    ────────────────────   │
    │         file_date         │
    │                           │
    │     1,877,582,955 rows    │
    │          (0.30s)          │
    └─────────────┬─────────────┘
    ┌─────────────┴─────────────┐
    │         TABLE_SCAN        │
    │    ────────────────────   │
    │         Function:         │
    │        PARQUET_SCAN       │
    │                           │
    │        Projections:       │
    │            day            │
    │           month           │
    │            year           │
    │                           │
    │   Total Files Read: 459   │
    │                           │
    │     1,877,582,955 rows    │
    │          (31.67s)         │
    └───────────────────────────┘

Uhhh what are the 459 files that it’s reading? There are well more than
1,000 files in the top-level directory so it’s not reading *all* of
those. But it’s clearly reading more than just the file I specified.
Running the same result with an un-transformed set of partition keys
results in about the same performance.

After reading [DuckDB’s
docs](https://duckdb.org/docs/stable/guides/performance/file_formats.html#the-effect-of-row-group-sizes),
I see that the internal structure of the Parquet files may be another
bottleneck:

> row group sizes \<5,000 have a strongly detrimental effect, making
> runtimes more than 5-10× larger than ideally-sized row groups, while
> row group sizes between 5,000 and 20,000 are still 1.5-2.5× off from
> best performance. Above row group size of 100,000, the differences are
> small: the gap is about 10% between the best and the worst runtime.

How large are the row group sizes for our data?

``` python
duckdb.sql("""
    SELECT
        min(row_group_num_rows),
        median(row_group_num_rows),
        mean(row_group_num_rows),
        max(row_group_num_rows)
    FROM parquet_metadata('s3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/*/*/*/*.parquet')
""")
```

    ┌─────────────────────────┬────────────────────────────┬──────────────────────────┬─────────────────────────┐
    │ min(row_group_num_rows) │ median(row_group_num_rows) │ mean(row_group_num_rows) │ max(row_group_num_rows) │
    │          int64          │           double           │          double          │          int64          │
    ├─────────────────────────┼────────────────────────────┼──────────────────────────┼─────────────────────────┤
    │                       0 │                    12710.0 │        19774.48645821066 │                  548658 │
    └─────────────────────────┴────────────────────────────┴──────────────────────────┴─────────────────────────┘

The good news for understanding what’s slowing query speeds is that
these row groups aren’t optimized; the good news is that at least half
of them are higher than 10,000 records, meaning that impacts ot
performance should be fairly small. But, even with small row-group
sizes, I’m surprised how long the `EXPLAIN` queries are taking since I
wouldn’t expect that individual row-group metadata is being read. I’m
feeling like DuckDB is still iterating through the row-group metadata
for each file even when it runs queries that filter on the partitioning
keys.

# Optimizing for speed

One way to address this could be to create an in-memory map
corresponding each key to each path. Since we make the file paths, we
can pre-perform and cache the work that DuckDB seems to be taking on
with each read:

``` python
duckdb.sql("""
CREATE OR REPLACE VIEW vehicle_positions AS
SELECT
    unnest(
        range(
            DATE '2025-01-01',
            date_add(current_date, INTERVAL 1 DAY),
            INTERVAL 1 DAY
        )
    ) :: DATE AS file_date,
    strftime(
        file_date,
        's3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/year=%Y/month=%-m/day=%-d/%xT%H_%M_%S.parquet'
    ) AS uri
""")
```

Now, we should be able to draw from this list when we read Parquet
files… but that acutally throws an error:

``` python
try:
    duckdb.sql("""
    SELECT *
    FROM read_parquet(
        (SELECT list(uri) FROM vehicle_positions
        WHERE file_date BETWEEN DATE '2025-09-01' AND DATE '2025-09-02')
    )
    """)
except Exception as e:
    print(e)
```

    Binder Error: Table function cannot contain subqueries

Instead, may we leverage DuckDB’s ability to create macros? Its docs
note that

> The CREATE MACRO statement can create a scalar or table macro
> (function) in the catalog.

That implies we can use it to read in new records.

Let’s write a new table macro that takes a date or date range and
returns s3 URIs:

``` python
duckdb.sql(
"""
CREATE OR REPLACE MACRO list_s3_uris

    (uri_format_string, date) AS strftime(
            date,
            uri_format_string
        ),

    (uri_format_string, start_date, end_date) AS list_transform(
        range(
            start_date,
            end_date,
            INTERVAL 1 DAY
        ),
        lambda x : strftime(
            x,
            uri_format_string
        )
    )
""")
```

Then, we can pipe the result of that into a table macro:

``` python
duckdb.sql(r"""
CREATE OR REPLACE MACRO vehicle_positions

    (date) AS TABLE (
        SELECT *
        FROM read_parquet(
            list_s3_uris(
                's3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/year=%Y/month=%-m/day=%-d/%xT%H:%M:%S.parquet',
                date
            )
        )
    ),

    (start_date, end_date) AS TABLE (
        SELECT *
        FROM read_parquet(
            list_s3_uris(
                's3://mbta-ctd-dataplatform-springboard/lamp/BUS_VEHICLE_POSITIONS/year=%Y/month=%-m/day=%-d/%xT%H:%M:%S.parquet',
                start_date,
                end_date
            )
        )
    )
""")
```

Now, does this return results faster?

``` python
tick = perf_counter()
duckdb.sql("""
    SELECT *
    FROM vehicle_positions(DATE '2025-09-26')
    LIMIT 10
""")
f"Query duration: {(perf_counter() - tick):.4f}s"
```

    'Query duration: 0.2039s'

It sure does! Computing the URIs for duckdb speeds up this query by a
hundredfold.

Does the speed stay quick for ranges of dates? Let’s try 7 days:

``` python
tick = perf_counter()
duckdb.sql("""
    SELECT *
    FROM vehicle_positions(DATE '2025-09-26', DATE '2025-10-03')
    LIMIT 10
""")
f"Query duration: {(perf_counter() - tick):.4f}s"
```

    'Query duration: 0.0160s'

I found [a
quote](https://github.com/duckdb/duckdb/issues/9474#issuecomment-1779864070)
from a DuckDB employee that explains this difference in runtime:

> The execution is done lazily - but binding is done immediately. In
> this case that means (1) resolving the glob and gathering a list of
> files to scan, and (2) reading the metadata of a Parquet file to
> figure out the names and types that exist in the file.

If we go back to the `EXPLAIN ANALYZE` query earlier, we saw that DuckDB
read the metadata of 450 files to construct this query. This quote also
tells me that duckdb is *not* storing the available filepaths when it
creates its view. Each time the view is created the globs are
re-resolved and the metadata read. Obviously, for our existing LAMP
data, this seems like a significant drawback. I understand that file
globs could change but it seems like a way to cache the results would
help.

This approach does come with its own set of caveats—I use this word
because I think the speed clearly outweighs them—

1.  LAMP will be responsible for updating these functions and loses out
    on upgrades to the file glob resolution.
2.  Users can’t query the minima or maxima of dates, they still need to
    know them ahead of time.
3.  LAMP defines how users filter the dates; we could overload the
    function to allow users to pass in a list of dates (eg. to get the
    VehiclePositions files from each Sunday) but this, too, requires
    some coding

Another consideration is how to handle different file structures; our
real-time data is partitioned by date but other data in
`mbta-ctd-dataplatform-springboard` are not partitioned and, generally,
have fewer files. Could the glob approach deliver better performance for
those datasets? To test, I’ll look at the `SHAPES/` dataset because it’s
one of the larger static files and has 630-odd partitions:

``` python
(
    duckdb
    .from_parquet("s3://mbta-ctd-dataplatform-springboard/lamp/SHAPES/*/*.parquet", hive_partitioning = True)
    .create_view("shapes", replace = True)
)
```

How quickly can we filter down a timestamp more recent than 2025?

``` python
tick = perf_counter()
duckdb.sql("""
SELECT *
FROM shapes
WHERE timestamp >= epoch(TIMESTAMP '2025-1-1')
""")
f"Query duration: {(perf_counter() - tick):.4f}s"
```

    'Query duration: 0.5810s'

About 2 times slower than when we construct the URIs but probably worth
the functionality and administrative benefits.

# A 2-prong solution

1.  table macros (functions) for datasets partitioned by date
2.  views over parquet globs for datasets partitioned by timestamp

# Some next steps

I also want to know if compacting our existing files into ones with
fewer row groups would make a meaningful difference in performance. To
test that, I want to read and rewrite a bunch of files in dev and see if
globbing those paths would help the speed of reads.
