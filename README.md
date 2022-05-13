# lamp
Lightweight Application for Measuring Performance

### Setup

Run the following:
```sh
asdf plugin-add poetry
asdf plugin-add python
asdf install
```

## Import Schema

- https_cdn.mbta.com_realtime_VehiclePositions_enhanced

    ```
    entity: list<item: struct>
        child 0, item: struct
            child 0, id: string
            child 1, vehicle: struct
                child 0, current_status: string
                child 1, current_stop_sequence: int64
                child 2, occupancy_percentage: int64
                child 3, occupancy_status: string
                child 4, position: struct
                    child 0, bearing: int64
                    child 1, latitude: double
                    child 2, longitude: double
                    child 3, speed: double
                child 5, stop_id: string
                child 6, timestamp: int64
                child 7, trip: struct
                    child 0, direction_id: int64
                    child 1, route_id: string
                    child 2, schedule_relationship: string
                    child 3, trip_id: string
                    child 4, start_date: string
                    child 5, start_time: string
                child 8, vehicle: struct
                    child 0, id: string
                    child 1, label: string
                    child 2, consist: list<item: struct>
                        child 0, item: struct
                            child 0, label: string
    header: struct
        child 0, gtfs_realtime_version: string
        child 1, incrementality: string
        child 2, timestamp: int64
    ```

- https_cdn.mbta.com_realtime_TripUpdates_enhanced
    ```
    entity: list<item: struct>
        child 0, item: struct
            child 0, id: string
            child 1, trip_update: struct
                child 0, stop_time_update: list<item: struct>
                    child 0, item: struct
                        child 0, departure: struct
                            child 0, time: int64
                            child 1, uncertainty: int64
                        child 1, stop_id: string
                        child 2, stop_sequence: int64
                        child 3, arrival: struct
                            child 0, time: int64
                            child 1, uncertainty: int64
                        child 4, schedule_relationship: string
                        child 5, boarding_status: string
                child 1, timestamp: int64
                child 2, trip: struct
                    child 0, direction_id: int64
                    child 1, route_id: string
                    child 2, start_date: string
                    child 3, start_time: string
                    child 4, trip_id: string
                    child 5, route_pattern_id: string
                    child 6, schedule_relationship: string
                child 3, vehicle: struct
                    child 0, id: string
                    child 1, label: string
    header: struct
        child 0, gtfs_realtime_version: string
        child 1, incrementality: string
        child 2, timestamp: int64
    ```

- https_cdn.mbta.com_realtime_Alerts_enhanced
    ```
    header: struct
        child 0, gtfs_realtime_version: string
        child 1, incrementality: int64
        child 2, timestamp: int64
    entity: list<item: struct>
        child 0, item: struct
            child 0, id: string
            child 1, alert: struct
                child 0, effect: string
                child 1, effect_detail: string
                child 2, cause: string
                child 3, cause_detail: string
                child 4, header_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 5, description_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 6, severity_level: string
                child 7, service_effect_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 8, short_header_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 9, severity: int64
                child 10, created_timestamp: int64
                child 11, last_modified_timestamp: int64
                child 12, timeframe_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 13, alert_lifecycle: string
                child 14, duration_certainty: string
                child 15, active_period: list<item: struct>
                    child 0, item: struct
                        child 0, start: int64
                        child 1, end: int64
                child 16, informed_entity: list<item: struct>
                    child 0, item: struct
                        child 0, stop_id: string
                        child 1, facility_id: string
                        child 2, activities: list<item: string>
                            child 0, item: string
                        child 3, agency_id: string
                        child 4, route_type: int64
                        child 5, route_id: string
                        child 6, trip: struct
                            child 0, route_id: string
                            child 1, trip_id: string
                            child 2, direction_id: int64
                        child 7, direction_id: int64
                child 17, url: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
                child 18, last_push_notification_timestamp: int64
                child 19, reminder_times: list<item: int64>
                    child 0, item: int64
                child 20, recurrence_text: struct
                    child 0, translation: list<item: struct>
                        child 0, item: struct
                            child 0, text: string
                            child 1, language: string
    ```