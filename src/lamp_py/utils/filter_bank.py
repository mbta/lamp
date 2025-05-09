import pyarrow.compute as pc
import pyarrow as pa


class FilterBankRtVehiclePositions:
    """
    Data-only class for pyarrow compute Expressions to filter Vehicle Positions
    """

    class ParquetFilter:
        """
        Class to contain parquet filters
        """

        green_b = pc.field("vehicle.trip.route_id") == "Green-B"
        green_c = pc.field("vehicle.trip.route_id") == "Green-C"
        green_d = pc.field("vehicle.trip.route_id") == "Green-D"
        green_e = pc.field("vehicle.trip.route_id") == "Green-E"
        mattapan = pc.field("vehicle.trip.route_id") == "Mattapan"

        green = pc.is_in(pc.field("vehicle.trip.route_id"), pa.array(["Green-B", "Green-C", "Green-D", "Green-E"]))
        orange = pc.field("vehicle.trip.route_id") == "Orange"
        blue = pc.field("vehicle.trip.route_id") == "Blue"
        red = pc.field("vehicle.trip.route_id") == "Red"

        # don't filter with these IDs - do this in Polars.
        # this is significantly slower
        light_rail_terminal_stop_ids = pa.array(list(map(str, [70110, 70162, 70236, 70274, 70502, 70510])))
        heavy_rail_terminal_stop_ids = pa.array(list(map(str, [70003, 70034, 70040, 70057, 70063, 70092, 70104])))
        light_rail_terminal_by_stop_id = pc.is_in(pc.field("vehicle.stop_id"), light_rail_terminal_stop_ids)
        heavy_rail_terminal_by_stop_id = pc.is_in(pc.field("vehicle.stop_id"), heavy_rail_terminal_stop_ids)

        light_rail = green_b | green_c | green_d | green_e | mattapan
        heavy_rail = orange | red | blue


class FilterBankRtTripUpdates:
    """
    Data-only class for pyarrow compute Expressions to filter Vehicle Positions
    """

    class ParquetFilter:
        """
        Class to contain parquet filters
        """

        green_b = pc.field("trip_update.trip.route_id") == "Green-B"
        green_c = pc.field("trip_update.trip.route_id") == "Green-C"
        green_d = pc.field("trip_update.trip.route_id") == "Green-D"
        green_e = pc.field("trip_update.trip.route_id") == "Green-E"
        mattapan = pc.field("trip_update.trip.route_id") == "Mattapan"

        green = pc.is_in(pc.field("trip_update.trip.route_id"), pa.array(["Green-B", "Green-C", "Green-D", "Green-E"]))
        orange = pc.field("trip_update.trip.route_id") == "Orange"
        blue = pc.field("trip_update.trip.route_id") == "Blue"
        red = pc.field("trip_update.trip.route_id") == "Red"

        # don't filter with these IDs - do this in Polars.
        # this is significantly slower
        light_rail_terminal_stop_ids = pa.array(
            list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))
        )
        heavy_rail_terminal_stop_ids = pa.array(list(map(str, [70001, 70036, 70038, 70059, 70061, 70094, 70105])))

        light_rail_terminal_by_stop_id = pc.is_in(
            pc.field("trip_update.stop_time_update.stop_id"), light_rail_terminal_stop_ids
        )
        heavy_rail_terminal_by_stop_id = pc.is_in(
            pc.field("trip_update.stop_time_update.stop_id"), heavy_rail_terminal_stop_ids
        )

        light_rail = green | mattapan
        heavy_rail = orange | red | blue
