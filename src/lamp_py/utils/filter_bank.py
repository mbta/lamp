from datetime import datetime
from typing import Optional
import pyarrow.compute as pc
import pyarrow as pa
import polars as pl

# GTFS_ARCHIVE = "s3://mbta-performance/lamp/gtfs_archive"
GTFS_ARCHIVE = "https://performancedata.mbta.com/lamp/gtfs_archive"


def list_station_child_stops_from_gtfs(
    stops: pl.DataFrame, parent_station: str, additional_filter: Optional[pl.Expr] = None
) -> pl.DataFrame:
    """
    Filter gtfs stops by parent_station string, and additional filter if available
    """
    df_parent_station = stops.filter(pl.col("parent_station") == parent_station)
    if additional_filter is not None:
        df_parent_station = df_parent_station.filter(additional_filter)
    return df_parent_station


class LightRailFilter:
    """
    Data-only class for lists of filters relevant for light rail
    """

    terminal_stop_ids = list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))


class HeavyRailFilter:
    """
    Data-only class for lists of filters relevant for heavy rail
    """

    service_date = datetime.now()

    # Multiple stop_id names avaiable for some stations
    _terminal_stop_place_names = [
        "place-forhl",
        "place-ogmnl",
        "place-bomnl",
        "place-wondl",
        "place-alfcl",
        "place-asmnl",
        "place-brntn",
    ]

    _terminal_stop_ids_numeric = list(map(str, [70001, 70036, 70038, 70059, 70061, 70094, 70105]))

    heavy_rail_filter = pl.col("vehicle_type") == 1

    service_date = datetime.now()
    stops = pl.read_parquet(f"{GTFS_ARCHIVE}/{service_date.year}/stops.parquet")

    terminal_stop_ids = []
    for place_name in _terminal_stop_place_names:
        gtfs_stops = list_station_child_stops_from_gtfs(stops, place_name, heavy_rail_filter)
        terminal_stop_ids.extend(gtfs_stops["stop_id"].to_list())


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
        _light_rail_terminal_stop_ids = pa.array(
            list(map(str, [70106, 70160, 70161, 70238, 70276, 70503, 70504, 70511, 70512]))
        )
        _heavy_rail_terminal_stop_ids = pa.array(list(map(str, [70001, 70036, 70038, 70059, 70061, 70094, 70105])))

        _light_rail_terminal_by_stop_id = pc.is_in(
            pc.field("trip_update.stop_time_update.stop_id"), _light_rail_terminal_stop_ids
        )
        _heavy_rail_terminal_by_stop_id = pc.is_in(
            pc.field("trip_update.stop_time_update.stop_id"), _heavy_rail_terminal_stop_ids
        )

        light_rail = green | mattapan
        heavy_rail = orange | red | blue
