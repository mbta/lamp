# Performance Manager

Performance Manager is an application to measure rail performance on the MBTA transit system.

# Database Schema

### `vehicle_events`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key for events |
| trip_hash | 16 bit binary | false | binary key used to join event record to trip data in `vehicle_trips` table |
| trip_stop_hash | 16 bit binary | false | binary key used to join trip-stop records during event processing |
| stop_id | string | false | |
| stop_sequence| small integer | false | |
| parent_station | string | false | |
| vp_move_timestamp | integer | true | earliest moving-status timestamp found from GTFS-RT Vehicle Position events |
| vp_stop_timestamp | integer | true | earliest stopped-status timestamp found from GTFS-RT Vehicle Position events |
| tu_stop_timestamp | integer | true | earliest timestamp found from GTFS-RT Trip Update events|
| updated_on | timestamp | false | timestamp field that is auto updated on any record change |

### `vehicle_trips`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| trip_hash | 16 bit binary | false | binary key used to join trip record to record in `vehicle_events` table |
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
| static_trip_id_guess | string | true | matched `trip_id` from GTFS `static_trips` table |
| static_start_time | integer | true | start_time of `static_trip_id_guess` trip |
| static_stop_count | small integer | true | expected stop count from `static_trip_id_guess` trip |
| first_last_station_match | boolean | false | true if `static_trip_id_guess` is exact match to `trip_id` |
| static_version_key | integer | false | GTFS static schedule version key for trip |
| updated_on | timestamp | false | timestamp field that is auto updated on any record change |

### `vehicle_event_metrics`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| trip_stop_hash | 16 bit binary | false | binary key used to join metrics records to events in `vehicle_events` table |
| travel_time_seconds | integer | true | seconds of time the vehicle spends traveling to station  |
| dwell_time_seconds | integer | true | seconds of time that vehicle spends waiting at station|
| headway_trunk_seconds | integer | true | departure to departure,  `parent_station` wait time for vehicles traveling on `trunk_route_id` |
| headway_branch_seconds | integer | true | departure to departure,  `parent_station` wait time for vehicles traveling on `branch_route_id` |
| updated_on | timestamp | false | timestamp field that is auto updated on any record change |

### `metadata_log`
| column name | data type | nullable | description |
| ----------- | --------- | -------- | ----------- |
| pk_id | integer | false | auto incremented primary key for S3 objects |
| processed | boolean | false | was the object processed by performance manager|
| process_fail | boolean | false | did an exception occur while processing |
| path | string | false | S3 object path |
| created_on | datetime | false | datetime field that is auto updated on record insert |

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


# Business Logic
The Performance Manager application regularly checks the `metadata_log` table for parquet files that have been created by the ingestion application. The order and business logic for processing is described below. This results from this process are recorded in the Performance Manager database.

## Loading GTFS Static Schedule
The Static Schedule is processed in two steps. First, we read the Feed Info, Trips, Routes, Stops, Stop Times, Calendar, Calendar Dates, and Directions parquet files from an individual static schedule. We use the static schedule partition structure to ensure all of the data is from the same schedule. The data is transformed to make types match with what the database is expecting and all data associated with bus routes are purged. The extraction only pulls out columns the database is expecting. This leaves us with Commuter Rail and Subway schedule data that is loaded into the database.

## Adding GTFS Static Metrics
We load the static schedule in so that we can compare it against our realtime data and create some adherence metrics. The comparison between realtime and scheduled can happen on stop times, but we also want to compare headways and travel times. In the static schedule, these metrics are not provided and have to be computed from scheduled stop times.

### Scheduled Travel Time
The Scheduled Travel Time is measured in whole seconds as the time it takes to leave one station and arrive at the current one. Its computed using an SQL Common Table Expression (CTE), that partitions all of the static stop times by `trip_id` and ordered by `stop_sequence`. It creates a travel time column by subtracting the departure time from the previous static stop time from the arrival time of the current stop time. The travel times are then loaded from this CTE into the Static Stop Times table.

### Scheduled Branch and Trunk Headways
The Headways are also stored as whole seconds and are generated for both a line's trunk and its branch. To compute, we use a temporary table in the RDS. For each static schedule, this table is truncated and repopulated with a record for each static stop time. Each static stop time primary key and departure time is joined with parent stations, service ids, directions, trunk route ids, and branch route ids from the Static Stops and Static Trips tables.

To calculate the Branch and Trunk Headways we again use CTEs, one for each type of headway. The CTE is is partitioned by parent station, service id, direction and either branch or trunk route id and sorted by departure time. A headway column is created by subtracting the previous record's departure time from the current record's departure time. The headways from both of these CTEs are then loaded into the Static Stop Times table.

## Loading GTFS Realtime Data
The GTFS Realtime Data is consumed from two different formats, the Vehicle Positions data and the Trip Updates data. When processing, we use all of the data from each format within the same hour. Each format is initially processed on its own before they are joined together for loading into the Trip Events and Trips tables in the database. For both datasets, we run some pre-processing steps that set datatypes correctly, add a static version key so that it can be joined with the correct static schedule, and remove bus records. Additionally, we prefer to work with a `parent_station` string instead of a `stop_id`, so we attempt to pull that parent station information out of the static schedule.

Vehicle Position data lists out time stamped positional data that is sourced from rail switches and gps data combined with vehicle, route, and station information. We receive a record with the last known position for each vehicle every few seconds. This raw data is compressed down by creating a `trip_stop_hash`, an md5 hash of the stop_sequence, parent_station, direction_id, route_id, service_date, start_time, and vehicle_id. The `trip_stop_hash` is unique to a trip going to and stopping at a station and are mapped to a single entry in the Vehicle Events table. To calculate the `vp_stop_timestamp` and `vp_move_timestamp` values for a Vehicle Event, we select the earliest event `vehicle_timestamp` (the vehicle clock when it reported its position) with a status indicating the vehicle is stopped and moving.

The Trip Updates data lists out predictions for when a train will arrive at a station. Again, an md5 `trip_stop_hash` is generated for each entry based on the stop_sequence, parent_station, direction_id, route_id, service_date, start_time and vehicle_id. As the columns and hashing algorithm are the same, the `trip_stop_hash`s in the trip updates dataset will match those from the the vehicle position dataset. For each hash, we use the last estimated arrival time as the `tu_stop_timestamp` as that one is going to be the closest to the actual arrival time.

Once both datasets have been processed, the three timestamps `vp_move_timestamp`, `vp_stop_timestamp`, and `tu_stop_timestamp` are joined based on the `trip_stop_hash`. We then insert new Vehicle Events into the database and update existing records.

## Adding GTFS Realtime Metrics
All of our metrics are calculated using our `vp_move_timestamp`, `vp_stop_timestamp`, and `tu_stop_timestamp`. As a rule, when our calculations require the time a vehicle has stopped at a station, we prefer to use the `vp_stop_timestamp` if it is available, falling back to the `tu_stop_timestamp` in instances when its not.

To calculate our metrics, we generate a CTE that contains the trip and stop information as well as the coalesced stop time information and flags for denoting if a station was the first or last stop on a trip. From this CTE, we create another CTE for each metric, pulling out the relevant information and adding in a metric column using the formulas described below. These metric CTEs are then joined together on `trip_stop_hash` and updated / inserted into the Vehicle Event Metrics table.

### Travel Times Calculation
Travel times are stored as whole seconds and represent the amount of time a vehicle spent moving to a station. They are calculated by subtracting the time a train started moving towards a station (the `vp_move_timestamp`) from the time a train arrived at the station (the `vp_stop_timestamp` or `tu_stop_timestamp`). For the stop time, we default to using the timestamp recorded in vehicle positions, falling back to the timestamp from trip updates. A travel time is generated for each `trip_stop_hash`.  If the `vp_move_timestamp` occurs after the `vp_stop_timestamp` or `tu_stop_timestamp`, which would result in a negative travel time, the travel time is not saved. 

```
# get stop and move timestamps from gtfs realtime data
t_stop = coalesce(vp_stop_timestamp, tu_stop_timestamp)
t_move = vp_move_timestamp

# calculate travel time from stop and move
t_travel = t_move - t_stop
```

### Dwell Times Calculation
Dwell times are stored as whole seconds and represent the amount of time a vehicle spent waiting at a station. For all stations except the first, this is the difference between timestamps when the vehicle starts moving towards the next station and when the vehicle stopped at the current station.

```
# get the stop and move timestamps from gtfs realtime data
t_stop = coalesce(vp_stop_timestamp, tu_stop_timestamp)
t_move = vp_move_timestamp "for next stop in trip"

# calculate the dwell time
t_dwell = t_stop - t_move
```

For the first stop on a trip however, we need to look at when the vehicle first arrived at the current stop, which is going to be the last stop on the previous trip. This is the time the last trip reached the terminus of the route, where it then turns around to back in the opposite direction.

```
# get the stop and move timestamps from gtfs realtime data
t_stop = coalesce(vp_stop_timestamp, tu_stop_timestamp) "for previous stop of vehicle"
t_move = vp_move_timestamp "for next stop in the trip"

# calculate the dwell time
t_dwell = t_stop - t_move
```

As with Travel Times, any negative Dwell Times are not saved.

### Headways Calculation

Headways are stored as whole seconds and represent the maximum platform wait time a rider would experience on a route. We've elected to describe our headways as departure to departure. This is the time between a vehicle leaving a `parent_station` and the next vehicle on the route leaving the same `parent_station`.

```
t_move_1 = vp_move_timestamp "for next station" "on this vehicle event"
t_move_0 = vp_move_timestamp "for next station" "on previous vehicle event at this station"

headway = t_move_1 - t_move_0
```

#### Branch Route ID Calculations
Headways are calculated for both the branch and trunk, using a `branch_route_id` and `trunk_route_id` value that is computed from raw `route_id` and `trip_id` GTFS Realtime data. These calculations are done by triggers when data is inserted into the Static Trips table. For the Orange and Blue lines, the branch route ids will be null and the trunk route ids will be "Orange" and "Blue". For the Green Line, that trunk route id will simply be "Green" and the branch routes will be copies of the route id (e.g. "Green-B"). For the Red line, the GTFS Realtime data has a route id of "Red", which we use for the trunk route id. The branch route id is determined by stop ids in the static stop times table for that trip's `trip_id`, resulting in "Red-Braintree" or "Red-Ashmont" values. If a Red line doesn't pass through any of the branch stations, its branch route id is null.


# Developer Usage

The Performance Manager application image is described in a [Dockerfile](../../../Dockerfile). This image is used for local testing and production deployment. 

The [docker-compose.yml](../../../../docker-compose.yml) file, found in the project root directory, describes a local environment for testing the performance manager application. This environment includes a local postgres database and `seed_metadata` application that can be used to configure / migrate the local database and seed it with selected AWS S3 object paths. 

To build application images run the following in the project root directory:
```sh
docker-compose build
```

To seed the local postgres database with AWS S3 objects paths, add selected parameters to `ENTRYPOINT` for the `seed_metadata` application in [docker-compose.yml](../../../../docker-compose.yml) and run the following in the project root directory:
```sh
docker-compose up seed_metadata
```
### `seed_metadata` Parameters
* `--clear-rt` (reset Real-Time RDS tables, leaving GTFS Static tables intact)
* `--clear-static` (reset Real-Time and Static RDS tables, full database reset)
* `--seed-file` (path to json file, inside of docker image, that will be loaded into `metadata_log` database table)

To start a local version of the performance manager application run the following in the project root directory:
```sh
docker-compose up performance_manager
```
The performance manager application will chronologically process any un-processed S3 object paths contained in the `metadata_log` database table.
