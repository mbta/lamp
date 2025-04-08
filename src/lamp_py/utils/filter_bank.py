import pyarrow.compute as pc
import pyarrow as pa

lrtp_terminals = pa.array([70110, 70162, 70236, 70274, 70502, 70510])


class FilterBankRtVehiclePositions:
    """
    Data-only class for pyarrow compute Expressions to filter Vehicle Positions
    """

    green_b = pc.field("vehicle.trip.route_id") == "Green-B"
    green_c = pc.field("vehicle.trip.route_id") == "Green-C"
    green_d = pc.field("vehicle.trip.route_id") == "Green-D"
    green_e = pc.field("vehicle.trip.route_id") == "Green-E"
    mattapan = pc.field("vehicle.trip.route_id") == "Mattapan"

    light_rail = green_b | green_c | green_d | green_e | mattapan
    green = green_b | green_c | green_d | green_e

    # todo - implement tags to filter all classes for specific sets
    tags = ["green", "light_rail"]

    revenue_true = pc.field("vehicle.trip.revenue") is True
    timestamp_non_null = pc.field("vehicle.timestamp").true_unless_null().all()
    terminal_stop_lr = pc.is_in(pc.field("vehicle.stop_id"), lrtp_terminals)

    light_rail_terminal_prediction_filter = revenue_true & timestamp_non_null & terminal_stop_lr


class FilterBankRtTripUpdates:
    """
    Data-only class for pyarrow compute Expressions to filter Vehicle Positions
    """

    green_b = pc.field("trip_update.trip.route_id") == "Green-B"
    green_c = pc.field("trip_update.trip.route_id") == "Green-C"
    green_d = pc.field("trip_update.trip.route_id") == "Green-D"
    green_e = pc.field("trip_update.trip.route_id") == "Green-E"
    mattapan = pc.field("trip_update.trip.route_id") == "Mattapan"

    light_rail = green_b | green_c | green_d | green_e | mattapan
    green = green_b | green_c | green_d | green_e

    schedule_relationship_not_skipped = pc.field("trip_update.trip.schedule_relationship").not_equal("CANCELED")
    revenue_true = pc.field("trip_update.trip.revenue") is True
    stop_time_update_schedule_relationship_not_skipped = pc.field(
        "trip_update.stop_time_update.schedule_relationship"
    ).not_equal("SKIPPED")
    timestamp_non_null = pc.field("trip_update.stop_time_update.schedule_relationship").not_equal("SKIPPED")
    stop_time_update_departure_time_non_null = (
        pc.field("trip_update.stop_time_update.departure.time").true_unless_null().all()
    )
    terminal_stop_lr = pc.is_in(pc.field("trip_update.stop_time_update.stop_id"), lrtp_terminals)

    light_rail_terminal_prediction_filter = (
        schedule_relationship_not_skipped
        & revenue_true
        & stop_time_update_schedule_relationship_not_skipped
        & timestamp_non_null
        & stop_time_update_departure_time_non_null
        & terminal_stop_lr
    )
    # todo - implement tags to filter all classes for specific sets
    tags = ["green", "light_rail"]
