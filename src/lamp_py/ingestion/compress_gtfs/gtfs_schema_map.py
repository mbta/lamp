from typing import Dict, List

import polars as pl

agency = {
    "agency_id": pl.Int64,
    "agency_name": pl.String,
    "agency_url": pl.String,
    "agency_timezone": pl.String,
    "agency_lang": pl.String,
    "agency_phone": pl.String,
}

areas = {
    "area_id": pl.String,
    "area_name": pl.String,
}

calendar = {
    "service_id": pl.String,
    "monday": pl.Int64,
    "tuesday": pl.Int64,
    "wednesday": pl.Int64,
    "thursday": pl.Int64,
    "friday": pl.Int64,
    "saturday": pl.Int64,
    "sunday": pl.Int64,
    "start_date": pl.Int64,
    "end_date": pl.Int64,
}

calendar_attributes = {
    "service_id": pl.String,
    "service_description": pl.String,
    "service_schedule_name": pl.String,
    "service_schedule_type": pl.String,
    "service_schedule_typicality": pl.Int64,
    "rating_start_date": pl.Int64,
    "rating_end_date": pl.Int64,
    "rating_description": pl.String,
}

calendar_dates = {
    "service_id": pl.String,
    "date": pl.Int64,
    "exception_type": pl.Int64,
    "holiday_name": pl.String,
}

checkpoints = {
    "checkpoint_id": pl.String,
    "checkpoint_name": pl.String,
}

directions = {
    "route_id": pl.String,
    "direction_id": pl.Int64,
    "direction": pl.String,
    "direction_destination": pl.String,
}

facilities = {
    "facility_id": pl.String,
    "facility_code": pl.String,
    "facility_class": pl.Int64,
    "facility_type": pl.String,
    "stop_id": pl.String,
    "facility_short_name": pl.String,
    "facility_long_name": pl.String,
    "facility_desc": pl.String,
    "facility_lat": pl.Float64,
    "facility_lon": pl.Float64,
    "wheelchair_facility": pl.Int64,
}

facilities_properties = {
    "facility_id": pl.String,
    "property_id": pl.String,
    "value": pl.String,
}

facilities_properties_definitions = {
    "property_id": pl.String,
    "definition": pl.String,
    "possible_values": pl.String,
}

fare_leg_rules = {
    "leg_group_id": pl.String,
    "network_id": pl.String,
    "from_area_id": pl.String,
    "to_area_id": pl.String,
    "fare_product_id": pl.String,
    "from_timeframe_group_id": pl.String,
    "to_timeframe_group_id": pl.String,
    "transfer_only": pl.Int64,
}

fare_media = {
    "fare_media_id": pl.String,
    "fare_media_name": pl.String,
    "fare_media_type": pl.Int64,
}

fare_products = {
    "fare_product_id": pl.String,
    "fare_product_name": pl.String,
    "fare_media_id": pl.String,
    "amount": pl.Float64,
    "currency": pl.String,
}

fare_transfer_rules = {
    "from_leg_group_id": pl.String,
    "to_leg_group_id": pl.String,
    "transfer_count": pl.Int64,
    "duration_limit": pl.Int64,
    "duration_limit_type": pl.Int64,
    "fare_transfer_type": pl.Int64,
    "fare_product_id": pl.String,
}

feed_info = {
    "feed_publisher_name": pl.String,
    "feed_publisher_url": pl.String,
    "feed_lang": pl.String,
    "feed_start_date": pl.Int64,
    "feed_end_date": pl.Int64,
    "feed_version": pl.String,
    "feed_contact_email": pl.String,
    "feed_id": pl.String,
}

levels = {
    "level_id": pl.String,
    "level_index": pl.Float64,
    "level_name": pl.String,
    "level_elevation": pl.String,
}

lines = {
    "line_id": pl.String,
    "line_short_name": pl.String,
    "line_long_name": pl.String,
    "line_desc": pl.String,
    "line_url": pl.String,
    "line_color": pl.String,
    "line_text_color": pl.String,
    "line_sort_order": pl.Int64,
}

linked_datasets = {
    "url": pl.String,
    "trip_updates": pl.Int64,
    "vehicle_positions": pl.Int64,
    "service_alerts": pl.Int64,
    "authentication_type": pl.Int64,
}

multi_route_trips = {
    "added_route_id": pl.String,
    "trip_id": pl.String,
}

pathways = {
    "pathway_id": pl.String,
    "from_stop_id": pl.String,
    "to_stop_id": pl.String,
    "facility_id": pl.Int64,
    "pathway_mode": pl.Int64,
    "is_bidirectional": pl.Int64,
    "length": pl.Float64,
    "wheelchair_length": pl.Float64,
    "traversal_time": pl.Int64,
    "wheelchair_traversal_time": pl.Int64,
    "stair_count": pl.Int64,
    "max_slope": pl.Float64,
    "pathway_name": pl.String,
    "pathway_code": pl.Float64,
    "signposted_as": pl.String,
    "instructions": pl.String,
}

route_patterns = {
    "route_pattern_id": pl.String,
    "route_id": pl.String,
    "direction_id": pl.Int64,
    "route_pattern_name": pl.String,
    "route_pattern_time_desc": pl.String,
    "route_pattern_typicality": pl.Int64,
    "route_pattern_sort_order": pl.Int64,
    "representative_trip_id": pl.String,
    "canonical_route_pattern": pl.Int64,
}

routes = {
    "route_id": pl.String,
    "agency_id": pl.Int64,
    "route_short_name": pl.String,
    "route_long_name": pl.String,
    "route_desc": pl.String,
    "route_type": pl.Int64,
    "route_url": pl.String,
    "route_color": pl.String,
    "route_text_color": pl.String,
    "route_sort_order": pl.Int64,
    "route_fare_class": pl.String,
    "line_id": pl.String,
    "listed_route": pl.Int64,
    "network_id": pl.String,
}

shapes = {
    "shape_id": pl.String,
    "shape_pt_lat": pl.Float64,
    "shape_pt_lon": pl.Float64,
    "shape_pt_sequence": pl.Int64,
    "shape_dist_traveled": pl.Float64,
}

stop_areas = {
    "stop_id": pl.String,
    "area_id": pl.String,
}

stops = {
    "stop_id": pl.String,
    "stop_code": pl.String,
    "stop_name": pl.String,
    "stop_desc": pl.String,
    "platform_code": pl.String,
    "platform_name": pl.String,
    "stop_lat": pl.Float64,
    "stop_lon": pl.Float64,
    "zone_id": pl.String,
    "stop_address": pl.String,
    "stop_url": pl.String,
    "level_id": pl.String,
    "location_type": pl.Int64,
    "parent_station": pl.String,
    "wheelchair_boarding": pl.Int64,
    "municipality": pl.String,
    "on_street": pl.String,
    "at_street": pl.String,
    "vehicle_type": pl.Int64,
}

stop_times = {
    "trip_id": pl.String,
    "arrival_time": pl.String,
    "departure_time": pl.String,
    "stop_id": pl.String,
    "stop_sequence": pl.Int64,
    "stop_headsign": pl.String,
    "pickup_type": pl.Int64,
    "drop_off_type": pl.Int64,
    "timepoint": pl.Int64,
    "checkpoint_id": pl.String,
    "continuous_pickup": pl.Int64,
    "continuous_drop_off": pl.Int64,
}

timeframes = {
    "timeframe_group_id": pl.String,
    "start_time": pl.String,
    "end_time": pl.String,
    "service_id": pl.String,
}

transfers = {
    "from_stop_id": pl.String,
    "to_stop_id": pl.String,
    "transfer_type": pl.Int64,
    "min_transfer_time": pl.Int64,
    "min_walk_time": pl.Int64,
    "min_wheelchair_time": pl.Int64,
    "suggested_buffer_time": pl.Int64,
    "wheelchair_transfer": pl.Int64,
    "from_trip_id": pl.String,
    "to_trip_id": pl.String,
}

trips = {
    "route_id": pl.String,
    "service_id": pl.String,
    "trip_id": pl.String,
    "trip_headsign": pl.String,
    "trip_short_name": pl.String,
    "direction_id": pl.Int64,
    "block_id": pl.String,
    "shape_id": pl.String,
    "wheelchair_accessible": pl.Int64,
    "trip_route_type": pl.Int64,
    "route_pattern_id": pl.String,
    "bikes_allowed": pl.Int64,
}

trips_properties = {
    "trip_id": pl.String,
    "trip_property_id": pl.String,
    "value": pl.String,
}

trips_properties_definitions = {
    "trip_property_id": pl.String,
    "definition": pl.String,
    "possible_values": pl.String,
}

schema_map: Dict[str, Dict] = {
    "agency.txt": agency,
    "areas.txt": areas,
    "calendar.txt": calendar,
    "calendar_attributes.txt": calendar_attributes,
    "calendar_dates.txt": calendar_dates,
    "checkpoints.txt": checkpoints,
    "directions.txt": directions,
    "facilities.txt": facilities,
    "facilities_properties.txt": facilities_properties,
    "facilities_properties_definitions.txt": facilities_properties_definitions,
    "fare_leg_rules.txt": fare_leg_rules,
    "fare_media.txt": fare_media,
    "fare_products.txt": fare_products,
    "fare_transfer_rules.txt": fare_transfer_rules,
    "feed_info.txt": feed_info,
    "levels.txt": levels,
    "lines.txt": lines,
    "linked_datasets.txt": linked_datasets,
    "multi_route_trips.txt": multi_route_trips,
    "pathways.txt": pathways,
    "route_patterns.txt": route_patterns,
    "routes.txt": routes,
    "shapes.txt": shapes,
    "stop_areas.txt": stop_areas,
    "stop_times.txt": stop_times,
    "stops.txt": stops,
    "timeframes.txt": timeframes,
    "transfers.txt": transfers,
    "trips.txt": trips,
    "trips_properties.txt": trips_properties,
    "trips_properties_definitions.txt": trips_properties_definitions,
}


def gtfs_schema(gtfs_table_file: str) -> Dict[str, pl.DataType]:
    """
    get schema of gtfs table file with polars datatypes

    :param gtfs_table_file: (ie. stop_times.txt)

    :return Dict[gtfs_table_field: polars DataType]
    """
    schema = schema_map.get(gtfs_table_file, None)
    if schema is not None:
        return schema.copy()

    raise IndexError(f"{gtfs_table_file} is not found in schema map")


def gtfs_schema_list() -> List[str]:
    """
    create list of all expected gtfs table files, with feed_info.txt at the end

    :return List[gtfs table files (ie. stop_times.txt)]
    """
    schema_list = list(schema_map.keys())
    schema_list.remove("feed_info.txt")
    schema_list.append("feed_info.txt")

    return schema_list
