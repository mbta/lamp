import pyarrow.compute as pc
import pyarrow as pa

lrtp_terminals = pa.array([70110, 70162, 70236, 70274, 70502, 70510])


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

        light_rail = green_b | green_c | green_d | green_e | mattapan
        green = green_b | green_c | green_d | green_e


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

        light_rail = green_b | green_c | green_d | green_e | mattapan
        green = green_b | green_c | green_d | green_e
