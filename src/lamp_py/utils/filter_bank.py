import pyarrow.compute as pc
from dataclasses import dataclass


class FilterBank_RtVehiclePositions:
    green_b = pc.field("vehicle.trip.route_id") == "Green-B"
    green_c = pc.field("vehicle.trip.route_id") == "Green-C"
    green_d = pc.field("vehicle.trip.route_id") == "Green-D"
    green_e = pc.field("vehicle.trip.route_id") == "Green-E"
    mattapan = pc.field("vehicle.trip.route_id") == "Mattapan"

    light_rail = green_b | green_c | green_d | green_e | mattapan
    green = green_b | green_c | green_d | green_e

    tags = ["green", "light_rail"]


# filter = filter1 | filter2 | filter3| filter4|filter5

# vpf = Rt_VehiclePositionFilters()

# filter3  = vpf.filter1 | vpf.filter2


class FilterBank_RtTripUpdates:
    green_b = pc.field("trip_update.trip.route_id") == "Green-B"
    green_c = pc.field("trip_update.trip.route_id") == "Green-C"
    green_d = pc.field("trip_update.trip.route_id") == "Green-D"
    green_e = pc.field("trip_update.trip.route_id") == "Green-E"
    mattapan = pc.field("trip_update.trip.route_id") == "Mattapan"

    light_rail = green_b | green_c | green_d | green_e | mattapan
    green = green_b | green_c | green_d | green_e
    tags = ["green", "light_rail"]


# filter = filter1 | filter2 | filter3| filter4|filter5

# vpf = Rt_VehiclePositionFilters()

# green(vehicle_positions : Optional[RtVehiclePositions])

#     return( vehicle_positions.filter(vpf.green)
#     out_tu = trip_updates.filter(tuf.green)
#     etc
#     etc
# grab all

# # polars stored procedures..
# filters()

# gr = filters.tag("green")

# gr.all_filters()
# # b,c,e
# gr.add_filter("bce", gr.green_b, gr.green_c, gr.green_e)


# pl.filter_by(gr.b | gr.c | gr.e)


# filter3  = vpf.filter1 | vpf.filter2
