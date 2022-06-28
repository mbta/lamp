## Parquet Schemas
Schemas for the parquet tables are gnerated using the `Configuration` objects
that write out the tables themselves.

Links to Parquet Schemas
* [Alerts](#alerts_format)
* [Bus Trip Updates](#bus_trip_updates_format)
* [Bus Vehicle Positions](#bus_vehicle_positions_format)
* [Schedule Data](#schedule_data_format)
* [Trip Updates](#trip_updates_format)
* [Vehicle Count](#vehicle_count_format)
* [Vehicle Positions](#vehicle_positions_format)

<a name='alerts_format'></a>
### Alerts
```txt
year: int16
month: int8
day: int8
hour: int8
feed_timestamp: int64
entity_id: string
effect: string
effect_detail: string
cause: string
cause_detail: string
severity: int64
severity_level: string
created_timestamp: int64
last_modified_timestamp: int64
alert_lifecycle: string
duration_certainty: string
last_push_notification: int64
active_period: list<
  item: struct<
    start: int64
    end: int64
  >
>
reminder_times: list<
  item: int64
>
closed_timestamp: int64
short_header_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
header_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
description_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
service_effect_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
timeframe_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
url_translation: list<
  item: struct<
    text: string
    language: string
  >
>
recurrence_text_translation: list<
  item: struct<
    text: string
    language: string
  >
>
informed_entity: list<
  item: struct<
    stop_id: string
    facility_id: string
    activities: list<
      item: string
    >
    agency_id: string
    route_type: int64
    route_id: string
    trip: struct<
      route_id: string
      trip_id: string
      direcction_id: int64
    >
    direction_id: int64
  >
>
```

<a name='bus_trip_updates_format'></a>
### Bus Trip Updates
TODO

<a name='bus_vehicle_positions_format'></a>
### Bus Vehicle Positions
```txt
year: int16
month: int8
day: int8
hour: int8
feed_timestamp: int64
entity_id: string
block_id: string
capacity: int64
current_stop_sequence: int64
load: int64
location_source: string
occupancy_percentage: int64
occupancy_status: string
revenue: bool
run_id: string
stop_id: string
vehicle_timestamp: int64
bearing: int64
latitude: double
longitude: double
speed: double
overload_id: int64
overload_offset: int64
route_id: string
schedule_relationship: string
start_date: string
trip_id: string
vehicle_id: string
vehicle_label: string
assignment_status: string
operator_id: string
logon_time: int64
name: string
first_name: string
last_name: string
```

<a name='schedule_data_format'></a>
### Schedule Data
The schedule data comes from a `MBTA_GTFS.zip` file. This zip contains a number
of tables encoded in dot txt files. These tables form a relational database, and
parquet files will be output directly copying this format, with an additional
timestamp field on each table that describes when the entire database was
uploaded to S3.  Documentation about the structure of these tables can be found
[here](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md).

<a name='trip_updates_format'></a>
### Trip Updates
```txt
year: int16
month: int8
day: int8
hour: int8
feed_timestamp: int64
entity_id: string
timestamp: int64
stop_time_update: list<
  item: struct<
    departure: struct<
      time: int64
      uncertainty: int64
    >
    stop_id: string
    stop_sequence: int64
    arrival: struct<
      time: int64
      uncertainty: int64
    >
    schedule_relationship: string
    boarding_status: string
  >
>
direction_id: int64
route_id: string
start_date: string
start_time: string
trip_id: string
route_pattern_id: string
schedule_relationship: string
vehicle_id: string
vehicle_label: string
```

<a name='vehicle_count_format'></a>
### Vehicle Count
TODO

<a name='vehicle_positions_format'></a>
### Vehicle Positions
```txt
year: int16
month: int8
day: int8
hour: int8
feed_timestamp: int64
entity_id: string
current_status: string
current_stop_sequence: int64
occupancy_percentage: int64
occupancy_status: string
stop_id: string
vehicle_timestamp: int64
bearing: int64
latitude: double
longitude: double
speed: double
direction_id: int64
route_id: string
schedule_relationship: string
start_date: string
start_time: string
trip_id: string
vehicle_id: string
vehicle_label: string
vehicle_consist: list<
  item: struct<
    label: string
  >
>
```
