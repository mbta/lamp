import dataframely as dy


position = dy.Struct(
    inner={
        "bearing": dy.UInt16(nullable=True),
        "latitude": dy.Float64(nullable=True),
        "longitude": dy.Float64(nullable=True),
        "speed": dy.Float64(nullable=True),
        "odometer": dy.Float64(nullable=True),
    }
)

trip_descriptor = dy.Struct(
    inner={
        "trip_id": dy.String(nullable=True),
        "route_id": dy.String(nullable=True),
        "direction_id": dy.UInt8(nullable=True),
        "start_time": dy.String(nullable=True),
        "start_date": dy.String(nullable=True),
        "schedule_relationship": dy.String(nullable=True),
        "route_pattern_id": dy.String(nullable=True),  # MBTA Enhanced Field
        "tm_trip_id": dy.String(nullable=True),  # Only used by Busloc
        "overload_id": dy.Int64(nullable=True),  # Only used by Busloc
        "overload_offset": dy.Int64(nullable=True),  # Only used by Busloc
        "revenue": dy.Bool(nullable=True),  # MBTA Enhanced Field
        "last_trip": dy.Bool(nullable=True),  # MBTA Enhanced Field
    }
)

vehicle_descriptor = dy.Struct(
    inner={
        "id": dy.String(nullable=False),
        "label": dy.String(nullable=True),
        "license_plate": dy.String(nullable=True),
        "consist": dy.List(
            dy.Struct(
                inner={
                    "label": dy.String(nullable=True),
                }
            ),
            nullable=True,
        ),  # MBTA Enhanced Field
        "assignment_status": dy.String(nullable=True),  # Only used by Busloc
    }
)

translated_string = dy.Struct(
    inner={
        "translation": dy.List(
            dy.Struct(
                inner={
                    "text": dy.String(nullable=True),
                    "language": dy.String(nullable=True),
                }
            ),
            nullable=True,
        )
    }
)

stop_time_event = dy.Struct(
    inner={
        "delay": dy.Int32(nullable=True),
        "time": dy.Int64(nullable=True),
        "uncertainty": dy.Int32(nullable=True),
    },
)
