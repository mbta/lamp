# Performance Manager

Performance Manager is an application to measure rail performance on the MBTA transit system.

# Database Schema

### `vehicle_events`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pm_event_id | integer | false | auto increment primary key |
| service_date | integer | false | |
| pm_trip_id | integer | false | integer key used to join event record to trip data in [vehicle_trips](#vehicle_trips) table
| stop_id | string | false | |
| stop_sequence | small integer | false | |
| canonical_stop_sequence | small integer | false | stop sequence based on "typical" route trip as defined in [route_patterns.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#route_patternstxt) table |
| sync_stop_sequence | small integer | false | stop_sequence that is consistent across all branches of a trunk for a particular `parent_station`|
| parent_station | string | false | |
| previous_trip_stop_pm_event_id | integer | true | pm_event_id of previous stop of pm_trip_id grouping |
| next_trip_stop_pm_event_id | integer | true| pm_event_id of next stop of pm_trip_id grouping |
| vp_move_timestamp | integer | true | earliest moving-status timestamp found from GTFS-RT Vehicle Position events |
| vp_stop_timestamp | integer | true | earliest stopped-status timestamp found from GTFS-RT Vehicle Position events |
| tu_stop_timestamp | integer | true | earliest timestamp found from GTFS-RT Trip Update events|
| travel_time_seconds | integer | true | seconds of time the vehicle spends traveling to station  |
| dwell_time_seconds | integer | true | seconds of time that vehicle spends waiting at station|
| headway_trunk_seconds | integer | true | departure to departure,  `parent_station` wait time for vehicles traveling on `trunk_route_id` |
| headway_branch_seconds | integer | true | departure to departure,  `parent_station` wait time for vehicles traveling on `branch_route_id` |
| updated_on | timestamp | false | timestamp field that is auto updated on any record change |

### `vehicle_trips`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pm_trip_id | integer | false | auto increment key used to join trips to records in [vehicle_events](#vehicle_events) table |
| direction_id| boolean | false | |
| route_id | string | false | |
| branch_route_id | string | true | |
| trunk_route_id | string | true | |
| service_date | integer | false | |
| start_time | integer | false | |
| vehicle_id | string | false | |
| stop_count | small integer | true | |
| trip_id | string | true | |
| vehicle_label | string | true | |
| vehicle_consist | string | true | |
| direction | string | true | |
| direction_destination | string | true | |
| static_trip_id_guess | string | true | matched `trip_id` from GTFS [static_trips](#static_trips) table |
| static_start_time | integer | true | start_time of `static_trip_id_guess` trip |
| static_stop_count | small integer | true | expected stop count from `static_trip_id_guess` trip |
| first_last_station_match | boolean | false | true if `static_trip_id_guess` is exact match to `trip_id` |
| static_version_key | integer | false | GTFS static schedule version key for trip |
| updated_on | timestamp | false | timestamp field that is auto updated on any record change |

### `static_feed_info`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| feed_start_date | integer | false | |
| feed_end_date | integer | false | |
| feed_version | string | false | |
| feed_active_date | integer | false | integer date pulled from `feed_version` value |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |
| created_on | datetime | false | datetime field that is auto updated on record insert |

### `static_trips`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| route_id | string | false | |
| branch_route_id | string | true | |
| trunk_route_id | string | true | |
| service_id | string | false | |
| trip_id | string | false | |
| direction_id | boolean | false | |
| block_id | string | true | | 
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_routes`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| route_id | string | false | |
| agency_id | small integer | false | |
| route_short_name | string | true | |
| route_long_name | string | true | |
| route_desc | string | true | |
| route_type | small integer | false | |
| route_sort_order | integer | false | |
| route_fare_class | string | false | |
| line_id | string | true | |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_stops`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| stop_id | string | false | |
| stop_name | string | false | |
| stop_desc | string | true | |
| platform_code | string | true | |
| platform_name | string | true | |
| parent_station | string | true |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_stop_times`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| trip_id | string | false |
| arrival_time | integer | false |
| departure_time | integer | false |
| scheduled_travel_time_seconds | integer | true | expected travel time for trip/stop generated by performance manager |
| scheduled_headway_trunk_seconds | integer | true | expected trunk headway for `parent_station` generated by performance manager |
| scheduled_headway_branch_seconds | integer | true | expected branch headways for `parent_station` generated by performance manager |
| stop_id | string | false |
| stop_sequence | small integer | false |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_calendar`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| service_id | string | false |
| monday | boolean | false |
| tuesday | boolean | false |
| wednesday | boolean | false |
| thursday | boolean | false |
| friday | boolean | false |
| saturday | boolean | false |
| sunday | boolean | false |
| start_date | integer | false |
| end_date | integer | false |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_calendar_dates`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| service_id | string | false |
| date | integer | false |
| exception_type | small integer | false |
| holiday_name | string | true |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_directions`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| route_id | string | false |
| direction_id | boolean | false |
| direction | string | false |
| direction_destination | string | false |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

### `static_route_patterns`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key |
| route_id | string | false |
| direction_id | boolean | false |
| route_pattern_typicality | smallint | true |
| representative_trip_id | string | false |
| static_version_key | integer | false | key used to link GTFS static schedule versions between tables |

# GTFS-RT Data

## General Information

[GTFS-realtime](https://www.mbta.com/developers/gtfs-realtime) (GTFS-RT) is provided by MBTA as an industry standard for distributing realtime transit data. 

The Performance Manager application uses two MBTA GTFS-RT feeds:

* [Vehicle Positions](https://gtfs.org/realtime/reference/#message-vehicleposition)
* [Trip Updates](https://gtfs.org/realtime/reference/#message-tripupdate)

Trip Updates are predictive data used to supplement Vehicle Positions data when the Vehicle Positions feed does not record an expected stop event.

All timestamp values used, or created by, Performance Manager are saved as whole second integers. 

## Data Loading

The CTD [Delta](https://github.com/mbta/delta) application is responsible for reading GTFS-RT updates from the MBTA [V3 API](https://www.mbta.com/developers/v3-api) and saving them to an AWS S3 Bucket, as gzipped JSON files, for use by LAMP.

The LAMP [Ingestion](../ingestion/README.md) application aggregates gzipped GTFS-RT update files, saved on S3 by Delta, into partitioned parquet files that are also saved to an S3 bucket. The parquet files are partitioned by GTFS-RT feed type and grouped into hourly chunks.

The Performance Manager application reads the GTFS-RT partitioned parquet files for the Vehicle Positions and Trip Updates feeds and passes them through an aggregation and manipulation pipeline before inserting them into the [vehicle_events](#vehicle_events) and [vehicle_trips](#vehicle_trips) table schemas described above.

When reading the GTFS-RT parquet files, records related to BUS data are removed to reduce database table sizes and processing time.

### Matching to GTFS Static Schedules

Performance Manager matches all GTFS-RT records to their applicable GTFS Static Schedule with the `static_version_key` field in the [vehicle_trips](#vehicle_trips) table. 

The `static_version_key` field allows GTFS-RT records to join to any GTFS Static data found in the `static_` database tables.

### Event Compression

The GTFS-RT partitioned parquet files have a large amount of redundant timestamp information that is not reasonable to save in an RDS database schema.

Performance Manager compresses GTFS-RT Vehicle Positions event records to store in the [vehicle_events](#vehicle_events) database table. 

Initially, Vehicle Positions events are grouped by unique trip-stop columns:
* parent_station
* route_id
* service_date
* trip_id

For each trip_stop event, the earliest `vehicle_timestamp` for a `current_status` indicating the vehicle is stopped is saved as the `vp_stop_timestamp` in the [vehicle_events](#vehicle_events) table. The earliest `vehicle_timestamp` for a `current_status` indicating the vehicle is moving is saved as the `vp_move_timestamp`.

This process is repeated for Trip Updates event records. Trip Updates can only record stopped status timestamps, so the earliest `arrival-timestamp` for each trip-stop event is saved as the `tu_stop_timestamp` in the [vehicle_events](#vehicle_events) table. 

For calculations/metrics that require a vehicle stop timestamp, Performance Manager defaults to using the `vp_stop_timestamp` value generated from the Vehicle Positions GTFS-RT feed. If a `vp_stop_timestamp` is not available for a trip-stop record, then the `tu_stop_timestamp` value is used.

## Metrics Business Logic

### Travel Times 
Travel times represent the amount of time a vehicle spent moving to the current station, from the previous station. 

Travel times are saved as `travel_time_seconds` in the [vehicle_events](#vehicle_events) table:

```
t_stop = coalesce(vp_stop_timestamp, tu_stop_timestamp)
t_move = vp_move_timestamp

travel_time_seconds = t_move - t_stop
```
Any `travel_time_seconds` calculated as a negative value is not saved. 

### Dwell Times 
Dwell times represent the amount of time a vehicle spent waiting at a station. A dwell time is not calculated for the last stop of a trip. The dwell time for the first stop of a trip is the duration since the previous vehicle trip stopped moving (including station turnaround time).

Dwell times are saved as `dwell_time_seconds` in the [vehicle_events](#vehicle_events) table:

```sh
# for first stop of trip
t_dwell_start = coalesce(vp_stop_timestamp, tu_stop_timestamp) # for previous stop of vehicle
# for NOT first stop of trip
t_dwell_start = coalesce(vp_stop_timestamp, tu_stop_timestamp) # for current stop of vehicle

t_station_depart = vp_move_timestamp # for next vehicle move event i.e. current station departure
t_dwell_end = t_station_depart

dwell_time_seconds = t_dwell_start - t_dwell_end
```

Any `dwell_time_seconds` calculated as a negative value is not saved. 

### Headways 

Headways represent the platform wait time a rider would experience on a route. This is a "departure-to-departure" calculation,  equivalent to the duration between a vehicle leaving a `parent_station` and the previous vehicle on the route leaving the same `parent_station` in the same direction. The first stop at a `parent_station` for each `service_date` will not have a calculated headway.

Headways are calculated for `branch_route_id` and `trunk_route_id` designations and saved as `headway_branch_seconds` and `headway_trunk_seconds`, respectively, in the [vehicle_events](#vehicle_events) table:

```sh
t_station_depart = vp_move_timestamp # for next vehicle move event i.e. current station departure
t_headway_start = t_station_depart

t_headway_end = t_station_depart # for previous vehicle at parent_station on same branch/trunk_route_id in same direction

headway_branch/trunk_seconds = t_headway_start - t_headway_end
```

If the `branch_route_id` for an event is `NULL`, then `headway_branch_seconds` will also be `NULL`


# GTFS Static Data

## Data Loading

[GTFS Static](https://www.mbta.com/developers/gtfs) Zip files are generated by MBTA for internal and external distribution. When a new GTFS Static Zip file is generated, the CTD [Delta](https://github.com/mbta/delta) application writes it to an AWS S3 bucket for use by LAMP.

The LAMP [Ingestion](../ingestion/README.md) application converts GTFS Zip files, saved on S3 by Delta, to partitioned parquet files that are also saved to an S3 bucket. 

The Performance Manager application reads the GTFS partitioned parquet files, from S3, and inserts the contents into the static [database schema](#database-schema) tables described above.

During database insertion, GTFS records related to BUS data are removed from the [static_routes](#static_routes), [static_trips](#static_trips), and [static_stop_times](#static_stop_times) tables. This is done to reduce database table sizes.

## Metrics Business Logic

For each GTFS Static schedule the following metrics are pre-calculated and stored in the [static_stop_times](#static_stop_times) table:

* scheduled_travel_time_seconds
* scheduled_headway_trunk_seconds
* scheduled_headway_branch_seconds

The business logic of these calculations follows the same rules as the GTFS-RT metrics, except that headway metrics are partitioned by `parent_station`, `service_id`, `direction_id` and `trunk/branch_route_id`. 


# Data Exports

For information regarding data exports, refer to [https://performancedata.mbta.com](https://performancedata.mbta.com) and our [Data Dictionary](../../../Data_Dictionary.md) documentation.


# Developer Usage

The Performance Manager application image is described in a [Dockerfile](../../../Dockerfile). This image is used for local testing and AWS deployment. 

The [docker-compose.yml](../../../docker-compose.yml) file, found in the project root directory, describes a local environment for testing the performance manager application. This environment includes a local PostgreSQL database and `seed_metadata` application that can be used to configure / migrate the local database and seed it with selected AWS S3 object paths. 

To build application images run the following in the project root directory:
```sh
docker-compose build
```

To seed the local PostgreSQL database with AWS S3 objects paths, add selected parameters to `ENTRYPOINT` for the `seed_metadata` application in [docker-compose.yml](../../../docker-compose.yml) and run the following in the project root directory:
```sh
docker-compose up seed_metadata
```
### `seed_metadata` Parameters:
* `--clear-rt` (reset Real-Time RDS tables, leaving GTFS Static tables intact)
* `--clear-static` (reset Real-Time and Static RDS tables, full database reset)
* `--seed-file` (path to json seed file, inside of docker image, that will be loaded into `metadata_log` table of the metadata RDS.)

To start a local version of the performance manager application run the following in the project root directory:
```sh
docker-compose up performance_manager
```
The performance manager application will chronologically process any un-processed S3 object paths contained in the `metadata_log` table of the metadata RDS.
