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


# --- GTFS Facility Utilities ---
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
