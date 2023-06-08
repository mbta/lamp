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
| process_fail | boolean | false | did an exception occcur while processing |
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

## GTFS-RT Event Compression

Performance Manager compresses GTFS-RT Vehicle Position event records for storage in the `vehicle_events` database table. 

The first step in the compression process is to group events by a generated `trip_stop_hash`. This hash is a md5 hash of the following event columns:

### `trip_stop_hash` columns
* stop_sequence
* parent_station
* direction_id
* route_id
* service_date
* start_time
* vehicle_id

For each `trip_stop_hash`, the eariest `vehicle_timestamp` for a status indicating the vehicle is stopped and any status indicating the vehicle is moving is saved. The stopped timestamp is saved as `vp_stop_timestamp` and the moving timestamp as `vp_move_timestamp`. 

This same process is applied to GTFS-RT Trip Update event records. Trip Updates can only record stopped status timestamps, so these values are saved as `tu_stop_timestamp` in the `vehicle_events` database table. 

These compressed timestamp records, along with stop and trip identifiers are used to create all metrics generated by the performance manager application. 

## Travel Times Calculation

Travel times are stored as whole seconds and represent the amount of time a vehicle spent moving to a station. 

Travel Times are calculated by: 
```
(vp_stop_timestamp or tu_stop_timestamp) - vp_move_timestamp
```
for each `trip_stop_hash`. 

If the `vp_move_timestamp` occurs after the `vp_stop_timestamp` or `tu_stop_timestamp`, which would result in a negative travel time, the travel time is not saved. 

## Dwell Times Calculation

Dwell times are stored as whole seconds and represent the amount of time a vehcile spent waiting at a station

Dwell Times are calculated by:
```sh
# first stop of trip
(vp_move_timestamp "for next stop of vehicle") - (vp_stop_timestamp or tu_stop_timestamp)

# all other stops on trip
(vp_move_timestamp "for next stop of vehicle") - (vp_stop_timestamp or tu_stop_timestamp "for previous stop of vehicle")
```
for each `trip_stop_hash`. 

Any Dwell Times that are calculated as negative are not saved. 

## Headways Calculation

Headways are stored as whole seconds and represent the maximum platform wait time a rider would experience on a route. This is the time between a vehicle leaving a parent_station and the next vehicle on the route leaving the same parent_station. 

Headways are calculated by `route_id` for `branch_route_id` and `trunk_route_id` designations. 

```sh
(vp_move_timestamp "for next station a.k.a. current station departure") - (vp_move_timestamp "for previous departure time at current station")
```
This is equivalent to a `departure to departure` headway calculation. 


# Developer Usage

The Performance Manager application image is described in a [Dockerfile](../../../Dockerfile). This image is used for local testing and production deployment. 

The [docker-compose.yml](../../../../docker-compose.yml) file, found in the project root directory, describes a local environment for testing the performance manager application. This evironment includes a local postgres database and `seed_metadata` application that can be used to configure / migrate the local database and seed it with selected AWS S3 object paths. 

To build application images run the following in the project root directory:
```sh
docker-compose build
```

To seed the local postges database with AWS S3 objects paths, add selected parameters to `ENTRYPOINT` for the `seed_metadata` aplication in [docker-compose.yml](../../../../docker-compose.yml) and run the following in the project root directory:
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
