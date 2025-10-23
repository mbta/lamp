# Why are vehicle labels missing for so many trips?
crunkel@mbta.com
2025-10-23

LAMP’s splunk logs
[detected](https://mbta.splunkcloud.com/en-US/app/search/search?q=search%20index%3D%22lamp-staging%22%20%22vehicle_label%7Cnullability%22&display.page.search.mode=smart&dispatch.sample_ratio=1&workload_pool=&earliest=%40d&latest=now&sid=1761257872.100058)
several thousand trips missing their vehicle labels. Here’s one such
trip, which I retrieved from prediction_loc:

    {
      "entity": [
        {
          "id": "y1619",
          "vehicle": {
            "current_status": "STOPPED_AT",
            "current_stop_sequence": 16,
            "occupancy_percentage": 0,
            "occupancy_status": "MANY_SEATS_AVAILABLE",
            "position": {
              "bearing": 225,
              "latitude": 42.27302169799805,
              "longitude": -71.14364624023438
            },
            "stop_id": "609",
            "timestamp": "2025-10-17 5:16:25 PM",
            "trip": {
              "start_time": "16:49:00",
              "direction_id": 0,
              "trip_id": "72094636",
              "route_id": "34E",
              "start_date": "20251017",
              "schedule_relationship": "SCHEDULED",
              "last_trip": false,
              "revenue": true
            },
            "vehicle": {
              "id": "y1619"
            }
          }
        }
      ],
      "header": {
        "timestamp": "2025-10-17 5:16:58 PM",
        "gtfs_realtime_version": "2.0",
        "incrementality": "FULL_DATASET"
      }
    }

You’ll note the `vehicle_id` is present in the data, but the `label`
field is missing. I then went back and looked at what BusLoc sent to
Concentrate. BusLoc—which should provide the first set of information to
Concentrate—displayed a `vehicle_label`:

    {
        "id": "1760735874_1619",
        "vehicle": {
            "position": {
                "speed": 1.1095,
                "latitude": 42.268909,
                "longitude": -71.148884,
                "bearing": 228
            },
            "timestamp": 1760735874,
    [...] # I removed the operator details for privacy
            "trip": {
                "overload_id": null,
                "schedule_relationship": "SCHEDULED",
                "route_id": "34E",
                "start_date": null,
                "trip_id": null,
                "tm_trip_id": "72094636",
                "overload_offset": null
            },
            "stop_id": null,
            "vehicle": {
                "id": "y1619",
                "label": "1619"
            },
            "location_source": "samsara",
            "revenue": true,
            "block_id": "B38-149",
            "current_stop_sequence": null,
            "run_id": "122-1414"
        },
        "is_deleted": false
    }

Swiftly also displayed a `vehicle_label` but it was equivalent to the
`vehicle_id`:

    {
      "header": {
        "gtfs_realtime_version": "2.0",
        "incrementality": 0,
        "timestamp": "2025-10-17 5:17:03 PM"
      },
      "entity": [
        {
          "id": "y1619",
          "vehicle": {
            "trip": {
              "trip_id": "72094636",
              "start_time": "16:49:00",
              "start_date": "20251017",
              "schedule_relationship": 0,
              "route_id": "34E",
              "direction_id": 0
            },
            "position": {
              "latitude": 42.27302932739258,
              "longitude": -71.14364624023438,
              "bearing": 225.0
            },
            "current_stop_sequence": 16,
            "current_status": 1,
            "timestamp": "2025-10-17 5:16:49 PM",
            "stop_id": "609",
            "vehicle": {
              "id": "y1619",
              "label": "y1619"
            },
            "occupancy_status": 1,
            "occupancy_percentage": 0
          }
        }
      ]
    }

I don’t understand:

1.  Why Swiftly sent a `vehicle_label` equivalent to the `vehicle_id`
2.  Why Concentrate didn’t populate `vehicle_label` from either BusLoc
    or Swiftly

I have one theory about number 2 but it doesn’t make sense *unless* the
response that prediction_loc provided from Swiftly wasn’t exactly what
Swiftly sent to Concentrate. My theory is that Swiftly did not send any
value for `vehicle_label` to Concentrate, which, in turn, didn’t
populate it in the GTFS-RT feed that LAMP polled. Concentrate [does
coalesce
values](https://github.com/mbta/concentrate/blob/1436e013a3b4ee5dea4f7dcbc157db3bcafb2b33/lib/concentrate/vehicle_position.ex#L106-L108)
for VehiclePositions from BusLoc and Swiftly but it does not coalesce
values in carriage details (you can run the test suite on [this
branch](https://github.com/mbta/concentrate/tree/debug-missing-vehicle-labels)
to see that behavior in action); if a partial carriage details object
exists in the more recent feed (Swiftly) it will overwrite the entire
carriage details object from the older feed (BusLoc). So if Swiftly sent
no `vehicle_label`, it would have overwritten the `vehicle_label` from
BusLoc with a null value.
