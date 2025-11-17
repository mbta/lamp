from enum import Enum

# https://gtfs.org/documentation/schedule/reference/#routestxt
# 0 - Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.
# 1 - Subway, Metro. Any underground rail system within a metropolitan area.
# 2 - Rail. Used for intercity or long-distance travel.
# 3 - Bus. Used for short- and long-distance bus routes.
# 4 - Ferry. Used for short- and long-distance boat service.


class RouteType(Enum):
    """
    RouteType enums to specify allowable values in downstream processing
    """

    LIGHT_RAIL = 0
    SUBWAY_METRO = 1
    COMMUTER_RAIL = 2
    BUS = 3
    FERRY = 4
