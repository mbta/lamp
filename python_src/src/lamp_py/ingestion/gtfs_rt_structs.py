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
        ("trip_id", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        ("route_id", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        ("direction_id", pyarrow.uint8()),
        ("start_time", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        ("start_date", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        (
            "schedule_relationship",
            pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()),
        ),
        (
            "route_pattern_id",
            pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()),
        ),  # MBTA Enhanced Field
        (
            "tm_trip_id",
            pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()),
        ),  # Only used by Busloc
        ("overload_id", pyarrow.int64()),  # Only used by Busloc
        ("overload_offset", pyarrow.int64()),  # Only used by Busloc
    ]
)

vehicle_descriptor = pyarrow.struct(
    [
        ("id", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        ("label", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        ("license_plate", pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())),
        (
            "consist",
            pyarrow.list_(
                pyarrow.struct(
                    [
                        ("label", pyarrow.string()),
                    ]
                ),
            ),
        ),  # MBTA Enhanced Field
        (
            "assignment_status",
            pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8()),
        ),  # Only used by Busloc
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
