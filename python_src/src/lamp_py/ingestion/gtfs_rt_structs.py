import pyarrow

position = pyarrow.struct(
    [
        ("bearing", pyarrow.uint16()),
        ("latitude", pyarrow.float64()),
        ("longitude", pyarrow.float64()),
        ("speed", pyarrow.float64()),
        ("odometer", pyarrow.float64()),
    ]
)

trip_descriptor = pyarrow.struct(
    [
        ("trip_id", pyarrow.string()),
        ("route_id", pyarrow.string()),
        ("direction_id", pyarrow.uint8()),
        ("start_time", pyarrow.string()),
        ("start_date", pyarrow.string()),
        ("schedule_relationship", pyarrow.string()),
        ("route_pattern_id", pyarrow.string()),
        ("tm_trip_id", pyarrow.string()),  # Only used by Busloc
        ("overload_id", pyarrow.int64()),  # Only used by Busloc
        ("overload_offset", pyarrow.int64()),  # Only used by Busloc
    ]
)

vehicle_descriptor = pyarrow.struct(
    [
        ("id", pyarrow.string()),
        ("label", pyarrow.string()),
        ("license_plate", pyarrow.string()),
        (
            "consist",
            pyarrow.list_(
                pyarrow.struct(
                    [
                        ("label", pyarrow.string()),
                    ]
                ),
            ),
        ),
        ("assignment_status", pyarrow.string()),  # Only used by Busloc
    ]
)

translated_string = pyarrow.struct(
    [
        (
            "translation",
            pyarrow.list_(
                pyarrow.struct(
                    [
                        ("text", pyarrow.string()),
                        ("language", pyarrow.string()),
                    ]
                )
            ),
        )
    ]
)

stop_time_event = pyarrow.struct(
    [
        ("delay", pyarrow.int32()),
        ("time", pyarrow.int64()),
        ("uncertainty", pyarrow.int32()),
    ]
)
