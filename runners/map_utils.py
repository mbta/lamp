"""
map_utils.py
Utilities for Marimo Boston Map Trip Finder
"""
import requests
import math
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict, Optional


# --- MBTA V3 API Utilities ---
def fetch_stops_v3(api_url: str = "https://api-v3.mbta.com/stops") -> List[Dict]:
    """
    Fetch stops from MBTA V3 API.
    
    Args:
        api_url: URL to MBTA V3 stops endpoint
        
    Returns:
        List of stop dicts with stop_id, stop_name, lat, lon
    """
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    stops = []
    
    for item in data.get("data", []):
        try:
            attrs = item.get("attributes", {})
            lat = attrs.get("latitude")
            lon = attrs.get("longitude")
            
            # Skip entries without valid coordinates
            if lat is None or lon is None:
                continue
                
            stops.append({
                "stop_id": item.get("id", ""),
                "stop_name": attrs.get("name", ""),
                "lat": float(lat),
                "lon": float(lon),
                "location_type": str(attrs.get("location_type", "")),
                "parent_station": attrs.get("parent_station", ""),
                "wheelchair_boarding": attrs.get("wheelchair_boarding", 0),
                "description": attrs.get("description", "")
            })
        except (ValueError, KeyError, TypeError):
            continue
    
    return stops


# --- GTFS Facility Utilities (legacy) ---
def fetch_gtfs_facilities(api_url: str) -> List[Dict]:
    """
    Fetch and parse GTFS stops from MBTA API.
    
    Args:
        api_url: URL to GTFS stops.txt file
        
    Returns:
        List of facility dicts with stop_id, stop_name, lat, lon
    """
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    
    stops = []
    reader = csv.DictReader(io.StringIO(response.text))
    
    for row in reader:
        try:
            lat = float(row.get("stop_lat", 0))
            lon = float(row.get("stop_lon", 0))
            
            # Skip entries without valid coordinates
            if lat == 0 or lon == 0:
                continue
                
            stops.append({
                "stop_id": row.get("stop_id", ""),
                "stop_name": row.get("stop_name", ""),
                "lat": lat,
                "lon": lon,
                "location_type": row.get("location_type", ""),
                "parent_station": row.get("parent_station", "")
            })
        except (ValueError, KeyError):
            continue
    
    return stops


def filter_facilities_by_type(facilities: List[Dict], location_type: str = "1") -> List[Dict]:
    """
    Filter facilities by location type.
    
    Args:
        facilities: List of facility dicts
        location_type: GTFS location_type (0=stop, 1=station, 2=entrance/exit)
        
    Returns:
        Filtered list of facilities
    """
    return [f for f in facilities if f.get("location_type") == location_type]


# --- Haversine Distance ---
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance in meters between two lat/lon points using Haversine formula.
    
    Args:
        lat1, lon1: First point coordinates
        lat2, lon2: Second point coordinates
        
    Returns:
        Distance in meters
    """
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


# --- Nearest Facility ---
def find_nearest_facility(
    lat: float, 
    lon: float, 
    facilities: List[Dict]
) -> Optional[Dict]:
    """
    Return facility closest to given lat/lon.
    
    Args:
        lat, lon: Search coordinates
        facilities: List of facility dicts with lat/lon
        
    Returns:
        Nearest facility dict or None if empty list
    """
    if not facilities:
        return None
        
    min_dist = float('inf')
    nearest = None
    for f in facilities:
        dist = haversine(lat, lon, f["lat"], f["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = f
    return nearest


# --- Prepare Plotly Data ---
def prepare_facility_plotly_data(facilities: List[Dict]) -> Dict:
    """
    Return Plotly scattermapbox data for facilities.
    
    Args:
        facilities: List of facility dicts
        
    Returns:
        Dict with lat, lon, text, customdata lists for Plotly
    """
    return {
        "lat": [f["lat"] for f in facilities],
        "lon": [f["lon"] for f in facilities],
        "text": [f["stop_name"] for f in facilities],
        "customdata": [f["stop_id"] for f in facilities],
    }


# --- Vehicle Search Utilities ---
def fetch_vehicle_positions(api_url: str) -> List[Dict]:
    """
    Fetch vehicle positions from GTFS-RT or lamp springboard.
    
    Args:
        api_url: URL to vehicle positions API
        
    Returns:
        List of vehicle dicts with vehicle_id, trip_id, lat, lon, timestamp
    """
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    # Parse JSON with vehicle positions
    vehicles = response.json().get("vehicles", [])
    return vehicles


def filter_vehicles_by_time(
    vehicles: List[Dict], 
    target_time: datetime, 
    window_minutes: int
) -> List[Dict]:
    """
    Filter vehicles within +/- window_minutes of target_time.
    
    Args:
        vehicles: List of vehicle dicts with timestamp field
        target_time: Center time for filtering
        window_minutes: Time window in minutes (applied +/-)
        
    Returns:
        Filtered list of vehicles within time window
    """
    results = []
    for v in vehicles:
        try:
            v_time = datetime.fromisoformat(v["timestamp"])
            if abs((v_time - target_time).total_seconds()) <= window_minutes * 60:
                results.append(v)
        except (ValueError, KeyError):
            continue
    return results


def find_nearest_vehicle(
    lat: float, 
    lon: float, 
    vehicles: List[Dict]
) -> Optional[Dict]:
    """
    Return vehicle closest to given lat/lon.
    
    Args:
        lat, lon: Search coordinates
        vehicles: List of vehicle dicts with lat/lon
        
    Returns:
        Nearest vehicle dict or None if empty list
    """
    if not vehicles:
        return None
        
    min_dist = float('inf')
    nearest = None
    for v in vehicles:
        dist = haversine(lat, lon, v["lat"], v["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = v
    return nearest


# --- Prepare Vehicle Plotly Data ---
def prepare_vehicle_plotly_data(vehicles: List[Dict]) -> Dict:
    """
    Return Plotly scattermapbox data for vehicles.
    
    Args:
        vehicles: List of vehicle dicts
        
    Returns:
        Dict with lat, lon, text, customdata lists for Plotly
    """
    return {
        "lat": [v["lat"] for v in vehicles],
        "lon": [v["lon"] for v in vehicles],
        "text": [v.get("vehicle_id", "") for v in vehicles],
        "customdata": [v.get("trip_id", "") for v in vehicles],
    }


# --- LAMP Springboard Vehicle Position Utilities ---

# Rail route IDs (no buses)
RAIL_ROUTE_IDS = [
    "Green-B", "Green-C", "Green-D", "Green-E",
    "Mattapan", "Red", "Blue", "Orange"
]

# Route colors for visualization
ROUTE_COLORS = {
    "Green-B": "#00843D",
    "Green-C": "#00843D",
    "Green-D": "#00843D",
    "Green-E": "#00843D",
    "Mattapan": "#DA291C",
    "Red": "#DA291C",
    "Blue": "#003DA5",
    "Orange": "#ED8B00",
}


def get_springboard_connection():
    """
    Create a DuckDB connection with lamp springboard catalog attached.
    
    Returns:
        DuckDB connection with lamp catalog attached
    """
    import duckdb
    
    conn = duckdb.connect()
    
    # Load AWS extension and use credential chain
    conn.execute("INSTALL aws; LOAD aws;")
    conn.execute("""
        CREATE SECRET (
            TYPE s3,
            PROVIDER credential_chain
        );
    """)
    
    # Attach lamp catalog
    conn.execute("""
        ATTACH 's3://mbta-ctd-dataplatform-staging-archive/lamp/catalog.db' AS lamp (READ_ONLY)
    """)
    
    return conn


def fetch_vehicle_positions_from_springboard(
    start_time: datetime,
    end_time: datetime,
    route_ids: List[str] = None
) -> List[Dict]:
    """
    Fetch vehicle positions from lamp springboard for a time range.
    
    Args:
        start_time: Start of time range
        end_time: End of time range  
        route_ids: List of route IDs to filter (defaults to RAIL_ROUTE_IDS)
        
    Returns:
        List of vehicle position dicts sorted by timestamp
    """
    if route_ids is None:
        route_ids = RAIL_ROUTE_IDS
    
    conn = get_springboard_connection()
    
    # Get date range for partitions
    start_date = start_time.date()
    end_date = end_time.date() + timedelta(days=1)  # read_ymd end is exclusive
    
    # Convert timestamps to unix epoch for filtering
    start_epoch = int(start_time.timestamp())
    end_epoch = int(end_time.timestamp())
    
    # Build route filter
    route_list = ", ".join([f"'{r}'" for r in route_ids])
    
    query = f"""
        SELECT 
            "vehicle.vehicle.id" as vehicle_id,
            "vehicle.vehicle.label" as vehicle_label,
            "vehicle.trip.trip_id" as trip_id,
            "vehicle.trip.route_id" as route_id,
            "vehicle.trip.direction_id" as direction_id,
            "vehicle.position.latitude" as lat,
            "vehicle.position.longitude" as lon,
            "vehicle.position.bearing" as bearing,
            "vehicle.timestamp" as timestamp,
            "vehicle.current_status" as current_status,
            "vehicle.stop_id" as stop_id
        FROM lamp.read_ymd(
            'RT_VEHICLE_POSITIONS',
            DATE '{start_date}',
            DATE '{end_date}'
        )
        WHERE "vehicle.trip.route_id" IN ({route_list})
        AND "vehicle.timestamp" >= {start_epoch}
        AND "vehicle.timestamp" <= {end_epoch}
        AND "vehicle.position.latitude" IS NOT NULL
        AND "vehicle.position.longitude" IS NOT NULL
        ORDER BY "vehicle.vehicle.id", "vehicle.timestamp"
    """
    
    result = conn.execute(query).fetchall()
    columns = ["vehicle_id", "vehicle_label", "trip_id", "route_id", "direction_id",
               "lat", "lon", "bearing", "timestamp", "current_status", "stop_id"]
    
    vehicles = []
    for row in result:
        vehicle = dict(zip(columns, row))
        # Convert timestamp to datetime
        if vehicle["timestamp"]:
            vehicle["timestamp_dt"] = datetime.fromtimestamp(vehicle["timestamp"])
        vehicles.append(vehicle)
    
    conn.close()
    return vehicles


def group_vehicle_positions_by_vehicle(positions: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group vehicle positions by vehicle ID.
    
    Args:
        positions: List of vehicle position dicts
        
    Returns:
        Dict mapping vehicle_id to list of positions (sorted by timestamp)
    """
    grouped = {}
    for pos in positions:
        vid = pos.get("vehicle_id", "unknown")
        if vid not in grouped:
            grouped[vid] = []
        grouped[vid].append(pos)
    
    # Sort each vehicle's positions by timestamp
    for vid in grouped:
        grouped[vid].sort(key=lambda x: x.get("timestamp", 0))
    
    return grouped


def calculate_opacity_for_trail(
    positions: List[Dict],
    min_opacity: float = 0.1,
    max_opacity: float = 1.0
) -> List[float]:
    """
    Calculate opacity values for a vehicle trail (older = more faded).
    
    Args:
        positions: List of positions sorted by timestamp (oldest first)
        min_opacity: Opacity for oldest point
        max_opacity: Opacity for newest point
        
    Returns:
        List of opacity values corresponding to each position
    """
    n = len(positions)
    if n == 0:
        return []
    if n == 1:
        return [max_opacity]
    
    # Linear interpolation from min to max opacity
    opacities = []
    for i in range(n):
        opacity = min_opacity + (max_opacity - min_opacity) * (i / (n - 1))
        opacities.append(opacity)
    return opacities
