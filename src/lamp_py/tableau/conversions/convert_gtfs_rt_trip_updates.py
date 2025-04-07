from polars import DataFrame
import pyarrow

gtfs_rt_trip_updates_processed_schema = pyarrow.schema(
    [
        ("id", pyarrow.large_string()),
        ("trip_update.trip.trip_id", pyarrow.large_string()),
        ("trip_update.trip.route_id", pyarrow.large_string()),
        ("trip_update.trip.direction_id", pyarrow.uint8()),
        ("trip_update.trip.start_time", pyarrow.large_string()),
        ("trip_update.trip.start_date", pyarrow.large_string()),
        ("trip_update.trip.schedule_relationship", pyarrow.large_string()),
        ("trip_update.trip.route_pattern_id", pyarrow.large_string()),
        ("trip_update.trip.tm_trip_id", pyarrow.large_string()),
        ("trip_update.trip.overload_id", pyarrow.int64()),
        ("trip_update.trip.overload_offset", pyarrow.int64()),
        ("trip_update.trip.revenue", pyarrow.bool_()),
        ("trip_update.trip.last_trip", pyarrow.bool_()),
        ("trip_update.vehicle.id", pyarrow.large_string()),
        ("trip_update.vehicle.label", pyarrow.large_string()),
        ("trip_update.vehicle.license_plate", pyarrow.large_string()),
        # trip_update.vehicle.consist: list<element: struct<label: string>>
        #   child 0, element: struct<label: string>
        #       child 0, label: string
        ("trip_update.vehicle.assignment_status", pyarrow.large_string()),
        ("trip_update.timestamp", pyarrow.uint64()),
        ("trip_update.delay", pyarrow.int32()),
        ("feed_timestamp", pyarrow.uint64()),
        ("trip_update.stop_time_update.stop_sequence", pyarrow.uint32()),
        ("trip_update.stop_time_update.stop_id", pyarrow.large_string()),
        ("trip_update.stop_time_update.arrival.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.arrival.time", pyarrow.int64()),
        ("trip_update.stop_time_update.arrival.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.delay", pyarrow.int32()),
        ("trip_update.stop_time_update.departure.time", pyarrow.int64()),
        ("trip_update.stop_time_update.departure.uncertainty", pyarrow.int32()),
        ("trip_update.stop_time_update.schedule_relationship", pyarrow.large_string()),
        ("trip_update.stop_time_update.boarding_status", pyarrow.large_string()),
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


def apply_gtfs_rt_trip_updates_conversions(polars_df: DataFrame) -> DataFrame:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    # # Convert datetime to Eastern Time
    # polars_df = polars_df.with_columns(
    #     pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
    #     pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
    #     pl.col("gtfs_travel_to_dt").dt.convert_time_zone(time_zone="US/Eastern").dt.replace_time_zone(None),
    # )

    # # Convert seconds columns to be aligned with Eastern Time
    # polars_df = polars_df.with_columns(
    #     (pl.col("gtfs_travel_to_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
    #     .dt.total_seconds()
    #     .alias("gtfs_travel_to_seconds"),
    #     (pl.col("stop_arrival_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
    #     .dt.total_seconds()
    #     .alias("stop_arrival_seconds"),
    #     (pl.col("stop_departure_dt") - pl.col("service_date").str.strptime(pl.Date, "%Y%m%d"))
    #     .dt.total_seconds()
    #     .alias("stop_departure_seconds"),
    # )

    # polars_df = polars_df.with_columns(pl.col("service_date").str.strptime(pl.Date, "%Y%m%d", strict=False))

    return polars_df


def schema() -> pyarrow.schema:
    return gtfs_rt_trip_updates_processed_schema
