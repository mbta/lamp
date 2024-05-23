# LAMP Data Exports

Access instructions for all LAMP public data exports are available at [https://performancedata.mbta.com](https://performancedata.mbta.com). 

LAMP currently produces the following sets of public data exports:
- [Subway Performance Data](#subway-performance-data)
- [OPMI Tableau Exports](#opmi-tableau-exports)

# Subway Performance Data

Each row represents a unique `trip_id`-`stop_id` pair for rail service.

| field name | type | description | source |
| ----------- | --------- | ----------- | ------------ |
| service_date | int64 | equivalent to GTFS-RT `start_date` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) as `int` instead of `string` | GTFS-RT |
| start_time | int64 |  equivalent to GTFS-RT `start_time` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) converted to seconds after midnight | GTFS-RT |
| route_id | string | equivalent to GTFS-RT `route_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | GTFS-RT |
| branch_route_id | string | equivalent to GTFS-RT `route_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) for lines with multiple routes, `NULL` if line has single route,  e.g. `Green-B` for `Green-B` route, `NULL` for `Blue` route | GTFS-RT |
| trunk_route_id | string | line if multiple routes exist on line, otherwise `route_id`,  e.g. `Green` for `Green-B` route, `Blue` for `Blue` route | GTFS-RT |
| trip_id | string | equivalent to GTFS-RT `trip_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | GTFS-RT |
| direction_id | bool | equivalent to GTFS-RT `direction_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) as `bool` instead of `int` | GTFS-RT |
| direction | string | equivalent to GTFS `direction` value from [directions.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#directionstxt) for `route_id`-`direction_id` pair | GTFS |
| direction_destination | string | equivalent to GTFS `direction_destination` value from [directions.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#directionstxt) for `route_id`-`direction_id` pair | GTFS |
| stop_count | int16 | number of stops recorded on trip | LAMP Calculated |
| vehicle_id | string | equivalent to GTFS-RT `id` value in [VehicleDescriptor](https://gtfs.org/realtime/reference/#message-vehicledescriptor) | GTFS-RT
| vehicle_label | string | equivalent to GTFS-RT `label` value in [VehicleDescriptor](https://gtfs.org/realtime/reference/#message-vehicledescriptor). | GTFS-RT
| vehicle_consist | string | Pipe separated concatenation of `multi_carriage_details` labels in [CarridageDetails](https://gtfs.org/realtime/reference/#message-carriagedetails) | GTFS-RT
| stop_id | string | equivalent to GTFS-RT `stop_id` value in [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition)| GTFS-RT |
| parent_station | string | `stop_name` of the `parent_station` associated with the `stop_id` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)  | GTFS |
| stop_sequence | int16 | equivalent to GTFS-RT `current_stop_sequence` value in [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) | GTFS-RT |
| move_timestamp | int64 | earliest "IN_TRANSIT_TO" or "INCOMING_AT" status `timestamp` for a trip-stop pair from GTFS-RT [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) | GTFS-RT |
| stop_timestamp | int64 | earliest "STOPPED_AT" status `timestamp` for a trip-stop pair from GTFS-RT [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) or last `arrival` timestamp from GTFS-RT [StopTimeUpdate](https://gtfs.org/realtime/reference/#message-stoptimeupdate) if VehiclePosition value is not available | GTFS-RT |
| travel_time_seconds | int64 | seconds the vehicle spent traveling to the `stop_id` of trip-stop pair from previous `stop_id` on trip | LAMP Calculated |
| dwell_time_seconds | int64 | seconds the vehicle spent stopped at `stop_id` of trip-stop pair | LAMP Calculated |
| headway_branch_seconds | int64 | seconds between consecutive vehicles departing `parent_station` on `branch_route_id` | LAMP Calculated |
| headway_trunk_seconds | int64 | seconds between consecutive vehicles departing `parent_station` on `trunk_route_id` | LAMP Calculated |
| scheduled_arrival_time | int64 | `arrival_time` of this trip-stop pair at `stop_id` for matched planned trip from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) | GTFS |
| scheduled_departure_time | int64 | `departure_time` of this trip-stop pair at `stop_id` for matched planned trip from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) | GTFS |
| scheduled_travel_time | int64 | planned seconds a vehicle spent traveling to the `stop_id` of trip-stop pair from previous `stop_id` on trip, derived from from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) | LAMP Calculated|
| scheduled_headway_branch | int64 | planned seconds between consecutive vehicles departing `parent_station` on `branch_route_id`, derived from from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) | LAMP Calculated |
| scheduled_headway_trunk | int64 | planned seconds between consecutive vehicles departing `parent_station` on `trunk_route_id`, derived from from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) | LAMP Calculated |


# OPMI Tableau Exports

The following LAMP data exports are used by [OPMI](https://www.massdottracker.com/wp/about/what-is-opmi-2/) for Tableau dashboarding:
- [LAMP_ALL_RT_field](#lamp_all_rt_fields)
- [LAMP_service_id_by_date_and_route](#lamp_service_id_by_date_and_route)
- [LAMP_static_calendar_dates](#lamp_static_calendar_dates)
- [LAMP_static_calendar](#lamp_static_calendar)
- [LAMP_static_feed_info](#lamp_static_feed_info)
- [LAMP_static_routes](#lamp_static_routes)
- [LAMP_static_stop_times](#lamp_static_stop_times)
- [LAMP_static_stops](#lamp_static_stops)
- [LAMP_static_trips](#lamp_static_trips)

## LAMP_ALL_RT_fields

Each row represents a unique `trip_id`-`stop_id` pair for rail service.

| field name | type | description | source |
| ----------- | --------- | ----------- | ------------ |
| service_date | date | equivalent to GTFS-RT `start_date` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) as `date` instead of `string` | GTFS-RT |
| start_datetime | datetime | equivalent to GTFS-RT `start_time` added to `start_date` from [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | Lamp Calculated |
| static_start_datetime | datetime | equivalent to `start_datetime` if planned trip, otherwise GTFS-RT `start_time` added to `static_start_time` | Lamp Calculated |
| stop_sequence | int16 | equivalent to GTFS-RT `current_stop_sequence` value in [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) | GTFS-RT |
| canonical_stop_sequence | int16 | stop sequence based on "canonical" route trip as defined in [route_patterns.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#route_patternstxt) table | Lamp Calculated |
| previous_canonical_stop_sequence | int16 | `canonical_stop_sequence` for previous stop on trip| Lamp Calculated |
| sync_stop_sequence | int16 | stop sequence that is consistent across all branches of a `trunk_route_id` for a particular `parent_station` | Lamp Calculated |
| previous_sync_stop_sequence | int16 | `sync_stop_sequence` for previous stop on trip | Lamp Calculated |
| stop_id | string | equivalent to GTFS-RT `stop_id` value in [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition)| GTFS-RT |
| previous_stop_id | string | `stop_id`  for previous stop on trip| GTFS-RT |
| parent_station | string | `stop_name` of the `parent_station` associated with the `stop_id` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)  | GTFS |
| previous_parent_station | string | `parent_station` for previous stop on trip| GTFS |
| stop_name | string | equivalent to GTFS `stop_name` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt) for `stop_id` | GTFS
| previous_stop_name | string | `stop_name` for previous stop on trip | GTFS
| previous_stop_departure_datetime | datetime | earliest "IN_TRANSIT_TO" OR "INCOMING_AT" status `timestamp` for a trip-stop pair from GTFS-RT [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) as an Eastern datetime | GTFS-RT
| stop_arrival_datetime | datetime | earliest "STOPPED_AT" status `timestamp` for a trip-stop pair from GTFS-RT [VehiclePosition](https://gtfs.org/realtime/reference/#message-vehicleposition) or last `arrival` timestamp from GTFS-RT [StopTimeUpdate](https://gtfs.org/realtime/reference/#message-stoptimeupdate) if VehiclePosition value is not available as an Eastern datetime | GTFS-RT
| stop_departure_datetime | datetime | equivalent to `previous_stop_departure_datetime` for next stop on trip | GTFS-RT
| previous_stop_departure_sec | int64 | `previous_stop_departure_datetime` as seconds after midnight | LAMP Calculated
| stop_arrival_sec | int64 | `stop_arrival_datetime` as seconds after midnight | LAMP Calculated
| stop_departure_sec | int64 | `stop_departure_datetime` as seconds after midnight | LAMP Calculated
| direction_id | int8 | equivalent to GTFS-RT `direction_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | GTFS-RT |
| route_id | string | equivalent to GTFS-RT `route_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | GTFS-RT |
| branch_route_id | string | equivalent to GTFS-RT `route_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) for lines with multiple routes, `NULL` if line has single route,  e.g. `Green-B` for `Green-B` route, `NULL` for `Blue` route_id | GTFS-RT |
| trunk_route_id | string | line if multiple routes exist on line, otherwise `route_id`,  e.g. `Green` for `Green-B` route, `Blue` for `Blue` route | GTFS-RT |
| start_time | int64 |  equivalent to GTFS-RT `start_time` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) converted to seconds after midnight | GTFS-RT |
| vehicle_id | string | equivalent to GTFS-RT `id` value in [VehicleDescriptor](https://gtfs.org/realtime/reference/#message-vehicledescriptor) | GTFS-RT
| stop_count | int16 | number of stops recorded on trip | LAMP Calculated |
| trip_id | string | equivalent to GTFS-RT `trip_id` value in [Trip Descriptor](https://gtfs.org/realtime/reference/#message-tripdescriptor) | GTFS-RT |
| vehicle_label | string | equivalent to GTFS-RT `label` value in [VehicleDescriptor](https://gtfs.org/realtime/reference/#message-vehicledescriptor). | GTFS-RT
| vehicle_consist | string | Pipe separated concatenation of `multi_carriage_details` labels in [CarridageDetails](https://gtfs.org/realtime/reference/#message-carriagedetails) | GTFS-RT
| direction | string | equivalent to GTFS `direction` value from [directions.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#directionstxt) for `route_id`-`direction_id` pair | GTFS |
| direction_destination | string | equivalent to GTFS `direction_destination` value from [directions.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#directionstxt) for `route_id`-`direction_id` pair | GTFS |
| static_trip_id_guess | string | `trip_id` if planned trip, otherwise closest matching `trip_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt) | LAMP Calculated
| static_start_time | int64 | earliest `arrival_time` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) for `static_trip_id_guess` | GTFS
| static_stop_count | int64 | planned stop count from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) of `static_trip_id_guess` trip | GTFS
| exact_static_trip_match | bool | `false` if `trip_id` is unplanned, otherwise `true` | LAMP Calculated
| static_version_key | int64 | internal LAMP foreign-key linking real-time events to static tables in [Database Schema](./src/lamp_py/performance_manager/README.md#database-schema) | LAMP Calculated
| travel_time_seconds | int64 | seconds the vehicle spent traveling to the `stop_id` of trip-stop pair from previous `stop_id` on trip | LAMP Calculated |
| dwell_time_seconds | int64 | seconds the vehicle spent stopped at `stop_id` of trip-stop pair | LAMP Calculated |
| headway_branch_seconds | int64 | seconds between consecutive vehicles departing `parent_station` on `branch_route_id` | LAMP Calculated |
| headway_trunk_seconds | int64 | seconds between consecutive vehicles departing `parent_station` on `trunk_route_id` | LAMP Calculated |

## LAMP_service_id_by_date_and_route

LAMP calculated dataset containing planned `route_id` and `service_id` combinations for each `service_date` in Tableau dataset.

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| route_id | string | `route_id` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| service_id | string | `service_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| service_date | int64 | date of service as `int` in "YYYYMMDD" format
| service_date_calc | date | date of service
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_calendar_dates

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| service_id | string | `service_id` from [calendar_dates.txt](https://gtfs.org/schedule/reference/#calendar_datestxt)
| date | int64 | `date` from [calendar_dates.txt](https://gtfs.org/schedule/reference/#calendar_datestxt) as `int` in "YYYYMMDD" format
| calendar_date | date | `date` from [calendar_dates.txt](https://gtfs.org/schedule/reference/#calendar_datestxt)
| exception_type | int8 | `exception_type` from [calendar_dates.txt](https://gtfs.org/schedule/reference/#calendar_datestxt)
| holiday_name | string | `holiday_name` from [calendar_dates.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#calendar_datestxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_calendar

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| service_id | string | `service_id` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt)
| monday | bool | `monday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| tuesday | bool | `tuesday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| wednesday | bool | `wednesday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| thursday | bool | `thursday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| friday | bool | `friday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| saturday | bool | `saturday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| sunday | bool | `sunday` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt) as bool
| start_date | date | `start_date` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt)
| end_date | date | `end_date` from [calendar.txt](https://gtfs.org/schedule/reference/#calendartxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_feed_info

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| feed_start_date | date | `feed_start_date` from [feed_info.text](https://gtfs.org/schedule/reference/#feed_infotxt)
| feed_end_date | date | `feed_end_date` from [feed_info.text](https://gtfs.org/schedule/reference/#feed_infotxt)
| feed_version | string | `feed_version` from [feed_info.text](https://gtfs.org/schedule/reference/#feed_infotxt)
| feed_active_date | date | date extracted from `feed_version` 
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_routes

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| route_id | string | `route_id` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| agency_id | int8 | `agency_id` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_short_name | string | `route_short_name` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_long_name | string | `route_long_name` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_desc | string | `route_desc` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_type | int8 | `route_type` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_sort_order | int32 | `route_sort_order` from [routes.txt](https://gtfs.org/schedule/reference/#routestxt)
| route_fare_class | string | `route_fare_class` from [routes.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#routestxt)
| line_id | string | `line_id` from [routes.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#routestxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_stop_times

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| trip_id | string | `trip_id` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt)
| arrival_time | int32 | `arrival_time` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) as seconds after midnight
| departure_time | int32 | `departure_time` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt) as seconds after midnight
| schedule_travel_time_seconds | int64 | (calculated) planned seconds the vehicle spent traveling to the `stop_id` of trip-stop pair from previous `stop_id` on trip
| schedule_headway_branch_seconds | int64 | (calculated) planned seconds between consecutive vehicles departing `stop_id` on `branch_route_id`
| schedule_headway_trunk_seconds | int64 | (calculated) planned seconds between consecutive vehicles departing `stop_id` on `trunk_route_id`
| stop_id | string | `stop_id` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt)
| stop_sequence | int16 | `stop_sequence` from [stop_times.txt](https://gtfs.org/schedule/reference/#stop_timestxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_stops

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| stop_id | string | `stop_id` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)
| stop_name | string | `stop_name` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)
| stop_desc | string | `stop_desc` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)
| platform_code | string | `platform_code` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)
| platform_name | string | `platform_name` from [stops.txt](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md#stopstxt)
| parent_station | string | `parent_station` from [stops.txt](https://gtfs.org/schedule/reference/#stopstxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |

## LAMP_static_trips

| field name | type | description |
| ----------- | --------- | ----------- |
| pk_id | int64 | LAMP primary key |
| route_id | string | `route_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| branch_route_id | string | `route_id` for lines with multiple routes, `NULL` if line has single route,  e.g. `Green-B` for `Green-B` route, `NULL` for `Blue` route 
| trunk_route_id | string | line if multiple routes exist on line, otherwise `route_id`,  e.g. `Green` for `Green-B` route, `Blue` for `Blue` route_id 
| service_id | string | `service_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| trip_id | string | `trip_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| direction_id | int8 | `direction_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| block_id | string | `block_id` from [trips.txt](https://gtfs.org/schedule/reference/#tripstxt)
| static_version_key | int64 | key used to link GTFS static schedule versions between tables |