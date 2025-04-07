import polars as pl
import pyarrow

gtfs_rt_vehicle_positions_processed_schema = pyarrow.schema(
    [
        ("id", pyarrow.large_string()),
        ("vehicle.trip.trip_id", pyarrow.large_string()),
        ("vehicle.trip.route_id", pyarrow.large_string()),
        ("vehicle.trip.direction_id", pyarrow.uint8()),
        ("vehicle.trip.start_time", pyarrow.large_string()),
        ("vehicle.trip.start_date", pyarrow.large_string()),
        ("vehicle.trip.schedule_relationship", pyarrow.large_string()),
        ("vehicle.trip.route_pattern_id", pyarrow.large_string()),
        ("vehicle.trip.tm_trip_id", pyarrow.large_string()),
        ("vehicle.trip.overload_id", pyarrow.int64()),
        ("vehicle.trip.overload_offset", pyarrow.int64()),
        ("vehicle.trip.revenue", pyarrow.bool_()),
        ("vehicle.trip.last_trip", pyarrow.bool_()),
        ("vehicle.vehicle.id", pyarrow.large_string()),
        ("vehicle.vehicle.label", pyarrow.large_string()),
        ("vehicle.vehicle.license_plate", pyarrow.large_string()),
        # vehicle.vehicle.consist: list<element: struct<label: string>>
        #   child 0, element: struct<label: string>
        #       child 0, label: string
        ("vehicle.vehicle.assignment_status", pyarrow.large_string()),
        ("vehicle.position.bearing", pyarrow.uint16()),
        ("vehicle.position.latitude", pyarrow.float64()),
        ("vehicle.position.longitude", pyarrow.float64()),
        ("vehicle.position.speed", pyarrow.float64()),
        ("vehicle.position.odometer", pyarrow.float64()),
        ("vehicle.current_stop_sequence", pyarrow.uint32()),
        ("vehicle.stop_id", pyarrow.large_string()),
        ("vehicle.current_status", pyarrow.large_string()),
        ("vehicle.timestamp", pyarrow.uint64()),
        ("vehicle.congestion_level", pyarrow.large_string()),
        ("vehicle.occupancy_status", pyarrow.large_string()),
        ("vehicle.occupancy_percentage", pyarrow.uint32()),
        # vehicle.multi_carriage_details: list<element: struct<id: string, label: string, occupancy_status: string, occupancy_percentage: int32, carriage_sequence: uint32>>
        #   child 0, element: struct<id: string, label: string, occupancy_status: string, occupancy_percentage: int32, carriage_sequence: uint32>
        #       child 0, id: string
        #       child 1, label: string
        #       child 2, occupancy_status: string
        #       child 3, occupancy_percentage: int32
        #       child 4, carriage_sequence: uint32
        ("feed_timestamp", pyarrow.uint64()),
    ]
)

# light_rail_events_dev_green_vehicle_pos = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/DEV_GREEN_RT_VEHICLE_POSITION"))
# light_rail_events_dev_green_trip_updates = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/DEV_GREEN_RT_TRIP_UPDATES"))

# light_rail_events_vehicle_pos = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/RT_VEHICLE_POSITIONS"))
# light_rail_events_trip_updates = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(LAMP, "light_rail_events/RT_TRIP_UPDATES"))

#     # filter=FilterBank_RtTripUpdates.light_rail,


#   s3_uris = file_list_from_s3(bucket_name=bus_events.bucket, file_prefix=bus_events.prefix)
#     ds_paths = [s.replace("s3://", "") for s in s3_uris]

#     if num_files is not None:
#         ds_paths = ds_paths[-num_files:]

#     ds = pd.dataset(
#         ds_paths,
#         format="parquet",
#         filesystem=S3FileSystem(),
#     )


def apply_gtfs_rt_vehicle_positions_conversions(polars_df: pl.DataFrame) -> pl.DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """

    return polars_df


def schema() -> pyarrow.schema:
    return gtfs_rt_vehicle_positions_processed_schema
