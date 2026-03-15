"""
test_map_utils.py
Unit tests for map_utils.py
"""
import pytest
from unittest.mock import patch, Mock
from datetime import datetime
import sys
import os

# Add runners to path for import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'runners'))
import map_utils


# --- Sample Data ---
SAMPLE_FACILITIES = [
    {"stop_id": "A", "stop_name": "Alpha Station", "lat": 42.35, "lon": -71.06},
    {"stop_id": "B", "stop_name": "Beta Station", "lat": 42.36, "lon": -71.07},
    {"stop_id": "C", "stop_name": "Gamma Station", "lat": 42.37, "lon": -71.08},
]

SAMPLE_VEHICLES = [
    {"vehicle_id": "V1", "trip_id": "T1", "lat": 42.35, "lon": -71.06, "timestamp": "2026-03-15T12:00:00"},
    {"vehicle_id": "V2", "trip_id": "T2", "lat": 42.36, "lon": -71.07, "timestamp": "2026-03-15T12:10:00"},
    {"vehicle_id": "V3", "trip_id": "T3", "lat": 42.37, "lon": -71.08, "timestamp": "2026-03-15T12:30:00"},
]

SAMPLE_GTFS_STOPS_CSV = """stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,wheelchair_boarding
place-north,NORTH,North Station,,42.365577,-71.06129,,https://www.mbta.com/stops/place-north,1,,1
place-south,SOUTH,South Station,,42.352271,-71.055242,,https://www.mbta.com/stops/place-south,1,,1
70061,,Park Street,,42.356395,-71.062424,,,0,place-pktrm,1
"""


class TestHaversine:
    """Tests for haversine distance calculation."""
    
    def test_haversine_positive_distance(self):
        """Distance between two different points should be positive."""
        d = map_utils.haversine(42.35, -71.06, 42.36, -71.07)
        assert d > 0
    
    def test_haversine_same_point(self):
        """Distance between same point should be zero."""
        d = map_utils.haversine(42.35, -71.06, 42.35, -71.06)
        assert d == 0
    
    def test_haversine_known_distance(self):
        """Test with known distance (Boston to NYC ~306km)."""
        # Boston: 42.3601, -71.0589
        # NYC: 40.7128, -74.0060
        d = map_utils.haversine(42.3601, -71.0589, 40.7128, -74.0060)
        assert 300000 < d < 320000  # ~306km
    
    def test_haversine_symmetry(self):
        """Distance should be same regardless of direction."""
        d1 = map_utils.haversine(42.35, -71.06, 42.36, -71.07)
        d2 = map_utils.haversine(42.36, -71.07, 42.35, -71.06)
        assert abs(d1 - d2) < 0.001


class TestFindNearestFacility:
    """Tests for nearest facility search."""
    
    def test_find_nearest_exact_match(self):
        """Should find facility at exact location."""
        nearest = map_utils.find_nearest_facility(42.35, -71.06, SAMPLE_FACILITIES)
        assert nearest["stop_id"] == "A"
    
    def test_find_nearest_closest(self):
        """Should find closest facility when not exact match."""
        # Point closer to A (42.35, -71.06) than B (42.36, -71.07)
        nearest = map_utils.find_nearest_facility(42.351, -71.061, SAMPLE_FACILITIES)
        assert nearest["stop_id"] == "A"
    
    def test_find_nearest_empty_list(self):
        """Should return None for empty facilities list."""
        nearest = map_utils.find_nearest_facility(42.35, -71.06, [])
        assert nearest is None
    
    def test_find_nearest_single_facility(self):
        """Should return single facility when only one exists."""
        single = [{"stop_id": "X", "stop_name": "X", "lat": 42.40, "lon": -71.10}]
        nearest = map_utils.find_nearest_facility(42.35, -71.06, single)
        assert nearest["stop_id"] == "X"


class TestPrepareFacilityPlotlyData:
    """Tests for Plotly data preparation."""
    
    def test_prepare_facility_data_structure(self):
        """Should return dict with required keys."""
        data = map_utils.prepare_facility_plotly_data(SAMPLE_FACILITIES)
        assert "lat" in data
        assert "lon" in data
        assert "text" in data
        assert "customdata" in data
    
    def test_prepare_facility_data_length(self):
        """All lists should have same length as input."""
        data = map_utils.prepare_facility_plotly_data(SAMPLE_FACILITIES)
        assert len(data["lat"]) == 3
        assert len(data["lon"]) == 3
        assert len(data["text"]) == 3
        assert len(data["customdata"]) == 3
    
    def test_prepare_facility_data_empty(self):
        """Should handle empty list."""
        data = map_utils.prepare_facility_plotly_data([])
        assert data["lat"] == []
        assert data["lon"] == []


class TestFilterVehiclesByTime:
    """Tests for vehicle time filtering."""
    
    def test_filter_all_within_window(self):
        """All vehicles within wide window should be returned."""
        target = datetime.fromisoformat("2026-03-15T12:15:00")
        filtered = map_utils.filter_vehicles_by_time(SAMPLE_VEHICLES, target, 60)
        assert len(filtered) == 3
    
    def test_filter_narrow_window(self):
        """Narrow window should filter out distant vehicles."""
        # ±5 min from 12:05 = 12:00-12:10, includes V1 (12:00) and V2 (12:10)
        target = datetime.fromisoformat("2026-03-15T12:05:00")
        filtered = map_utils.filter_vehicles_by_time(SAMPLE_VEHICLES, target, 5)
        assert len(filtered) == 2
        
        # Narrower test: ±2 min from 12:01 only includes V1
        target2 = datetime.fromisoformat("2026-03-15T12:01:00")
        filtered2 = map_utils.filter_vehicles_by_time(SAMPLE_VEHICLES, target2, 2)
        assert len(filtered2) == 1
        assert filtered2[0]["vehicle_id"] == "V1"
    
    def test_filter_medium_window(self):
        """Medium window test."""
        target = datetime.fromisoformat("2026-03-15T12:05:00")
        filtered = map_utils.filter_vehicles_by_time(SAMPLE_VEHICLES, target, 15)
        assert len(filtered) == 2
    
    def test_filter_empty_vehicles(self):
        """Should handle empty vehicle list."""
        target = datetime.fromisoformat("2026-03-15T12:00:00")
        filtered = map_utils.filter_vehicles_by_time([], target, 15)
        assert filtered == []
    
    def test_filter_invalid_timestamp(self):
        """Should skip vehicles with invalid timestamps."""
        vehicles_with_bad = SAMPLE_VEHICLES + [{"vehicle_id": "BAD", "timestamp": "invalid"}]
        target = datetime.fromisoformat("2026-03-15T12:05:00")
        filtered = map_utils.filter_vehicles_by_time(vehicles_with_bad, target, 60)
        assert len(filtered) == 3  # Only valid vehicles


class TestFindNearestVehicle:
    """Tests for nearest vehicle search."""
    
    def test_find_nearest_vehicle_exact(self):
        """Should find vehicle at exact location."""
        nearest = map_utils.find_nearest_vehicle(42.36, -71.07, SAMPLE_VEHICLES)
        assert nearest["vehicle_id"] == "V2"
    
    def test_find_nearest_vehicle_closest(self):
        """Should find closest vehicle."""
        nearest = map_utils.find_nearest_vehicle(42.365, -71.075, SAMPLE_VEHICLES)
        assert nearest["vehicle_id"] in ["V2", "V3"]  # Between V2 and V3
    
    def test_find_nearest_vehicle_empty(self):
        """Should return None for empty list."""
        nearest = map_utils.find_nearest_vehicle(42.35, -71.06, [])
        assert nearest is None


class TestPrepareVehiclePlotlyData:
    """Tests for vehicle Plotly data preparation."""
    
    def test_prepare_vehicle_data_structure(self):
        """Should return dict with required keys."""
        data = map_utils.prepare_vehicle_plotly_data(SAMPLE_VEHICLES)
        assert "lat" in data
        assert "lon" in data
        assert "text" in data
        assert "customdata" in data
    
    def test_prepare_vehicle_data_values(self):
        """Should extract correct values."""
        data = map_utils.prepare_vehicle_plotly_data(SAMPLE_VEHICLES)
        assert data["text"] == ["V1", "V2", "V3"]
        assert data["customdata"] == ["T1", "T2", "T3"]


class TestFetchGtfsFacilities:
    """Tests for GTFS facility fetching."""
    
    @patch('map_utils.requests.get')
    def test_fetch_gtfs_facilities_success(self, mock_get):
        """Should parse GTFS stops.txt correctly."""
        mock_response = Mock()
        mock_response.text = SAMPLE_GTFS_STOPS_CSV
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        facilities = map_utils.fetch_gtfs_facilities("http://example.com/stops.txt")
        
        assert len(facilities) == 3
        assert facilities[0]["stop_id"] == "place-north"
        assert facilities[0]["stop_name"] == "North Station"
        assert abs(facilities[0]["lat"] - 42.365577) < 0.0001
    
    @patch('map_utils.requests.get')
    def test_fetch_gtfs_facilities_empty(self, mock_get):
        """Should handle empty response."""
        mock_response = Mock()
        mock_response.text = "stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon\n"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        facilities = map_utils.fetch_gtfs_facilities("http://example.com/stops.txt")
        assert facilities == []


class TestFilterFacilitiesByType:
    """Tests for facility type filtering."""
    
    def test_filter_by_station_type(self):
        """Should filter by location_type."""
        facilities_with_types = [
            {"stop_id": "A", "stop_name": "A", "lat": 42.35, "lon": -71.06, "location_type": "1"},
            {"stop_id": "B", "stop_name": "B", "lat": 42.36, "lon": -71.07, "location_type": "0"},
            {"stop_id": "C", "stop_name": "C", "lat": 42.37, "lon": -71.08, "location_type": "1"},
        ]
        
        stations = map_utils.filter_facilities_by_type(facilities_with_types, "1")
        assert len(stations) == 2
        
        stops = map_utils.filter_facilities_by_type(facilities_with_types, "0")
        assert len(stops) == 1


# Sample V3 API response
SAMPLE_V3_API_RESPONSE = {
    "data": [
        {
            "id": "place-north",
            "type": "stop",
            "attributes": {
                "name": "North Station",
                "latitude": 42.365577,
                "longitude": -71.06129,
                "location_type": 1,
                "wheelchair_boarding": 1,
                "description": "North Station - Commuter Rail"
            }
        },
        {
            "id": "place-sstat",
            "type": "stop",
            "attributes": {
                "name": "South Station",
                "latitude": 42.352271,
                "longitude": -71.055242,
                "location_type": 1,
                "wheelchair_boarding": 1,
                "description": "South Station - Commuter Rail"
            }
        },
        {
            "id": "70061",
            "type": "stop",
            "attributes": {
                "name": "Park Street",
                "latitude": 42.356395,
                "longitude": -71.062424,
                "location_type": 0,
                "wheelchair_boarding": 1,
                "description": None
            }
        }
    ]
}


class TestFetchStopsV3:
    """Tests for MBTA V3 API stop fetching."""
    
    @patch('map_utils.requests.get')
    def test_fetch_stops_v3_success(self, mock_get):
        """Should parse V3 API response correctly."""
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_V3_API_RESPONSE
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        stops = map_utils.fetch_stops_v3("https://api-v3.mbta.com/stops")
        
        assert len(stops) == 3
        assert stops[0]["stop_id"] == "place-north"
        assert stops[0]["stop_name"] == "North Station"
        assert abs(stops[0]["lat"] - 42.365577) < 0.0001
        assert abs(stops[0]["lon"] - (-71.06129)) < 0.0001
    
    @patch('map_utils.requests.get')
    def test_fetch_stops_v3_empty(self, mock_get):
        """Should handle empty API response."""
        mock_response = Mock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        stops = map_utils.fetch_stops_v3("https://api-v3.mbta.com/stops")
        assert stops == []
    
    @patch('map_utils.requests.get')
    def test_fetch_stops_v3_missing_coords(self, mock_get):
        """Should skip stops without coordinates."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [
                {
                    "id": "stop-no-coords",
                    "type": "stop",
                    "attributes": {
                        "name": "No Coords Stop",
                        "latitude": None,
                        "longitude": None
                    }
                },
                {
                    "id": "stop-with-coords",
                    "type": "stop",
                    "attributes": {
                        "name": "Has Coords",
                        "latitude": 42.35,
                        "longitude": -71.06
                    }
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        stops = map_utils.fetch_stops_v3("https://api-v3.mbta.com/stops")
        assert len(stops) == 1
        assert stops[0]["stop_id"] == "stop-with-coords"
