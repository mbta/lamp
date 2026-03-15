"""
Boston Map Trip Finder - Marimo Notebook
Interactive map for finding nearest MBTA facilities or vehicles.
"""
import marimo

app = marimo.App(width="full")


@app.cell
def __():
    """Imports and dependencies."""
    import marimo as mo
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    import map_utils
    return datetime, go, map_utils, mo, timedelta


@app.cell
def __(mo):
    """Search mode radio buttons."""
    search_mode = mo.ui.radio(
        options=["Nearest Facility", "Nearest Vehicle"],
        value="Nearest Facility",
        label="Search Mode"
    )
    search_mode
    return (search_mode,)


@app.cell
def __(datetime, mo):
    """Time picker for vehicle search."""
    time_picker = mo.ui.datetime(
        value=datetime.now(),
        label="Select Time"
    )
    time_picker
    return (time_picker,)


@app.cell
def __(mo):
    """Time range selector for vehicle search."""
    time_range = mo.ui.radio(
        options=["±5 min", "±15 min", "±60 min"],
        value="±15 min",
        label="Time Range"
    )
    time_range
    return (time_range,)


@app.cell
def __(search_mode, time_picker, time_range, mo):
    """Conditionally show time controls only for vehicle search."""
    if search_mode.value == "Nearest Vehicle":
        time_controls = mo.vstack([
            mo.md("### Vehicle Search Options"),
            time_picker,
            time_range
        ])
    else:
        time_controls = mo.md("*Select 'Nearest Vehicle' to enable time controls*")
    time_controls
    return (time_controls,)


@app.cell
def __(map_utils):
    """Fetch GTFS facilities from MBTA API."""
    MBTA_STOPS_URL = "https://cdn.mbta.com/MBTA_GTFS/stops.txt"
    try:
        facilities = map_utils.fetch_gtfs_facilities(MBTA_STOPS_URL)
        facilities_status = f"Loaded {len(facilities)} facilities"
    except Exception as e:
        facilities = []
        facilities_status = f"Error loading facilities: {e}"
    return MBTA_STOPS_URL, facilities, facilities_status


@app.cell
def __(facilities_status, mo):
    """Display facilities loading status."""
    mo.md(f"**Facilities Status:** {facilities_status}")
    return ()


@app.cell
def __(go, facilities, map_utils):
    """Create the Boston map with facilities."""
    def create_boston_map(facilities_list, selected_point=None, result_point=None):
        fig = go.Figure()
        
        # Add facility markers
        if facilities_list:
            facility_data = map_utils.prepare_facility_plotly_data(facilities_list)
            fig.add_trace(go.Scattermapbox(
                lat=facility_data["lat"],
                lon=facility_data["lon"],
                text=facility_data["text"],
                customdata=facility_data["customdata"],
                mode="markers",
                marker=dict(size=8, color="blue", opacity=0.6),
                name="Facilities",
                hovertemplate="<b>%{text}</b><br>ID: %{customdata}<extra></extra>"
            ))
        
        # Add selected point marker
        if selected_point:
            fig.add_trace(go.Scattermapbox(
                lat=[selected_point[0]],
                lon=[selected_point[1]],
                mode="markers",
                marker=dict(size=15, color="red"),
                name="Selected Point",
                hovertemplate="Selected<br>Lat: %{lat:.4f}<br>Lon: %{lon:.4f}<extra></extra>"
            ))
        
        # Add result point marker
        if result_point:
            fig.add_trace(go.Scattermapbox(
                lat=[result_point["lat"]],
                lon=[result_point["lon"]],
                mode="markers",
                marker=dict(size=12, color="green", symbol="star"),
                name="Nearest Result",
                hovertemplate="<b>%{text}</b><extra></extra>",
                text=[result_point.get("name", "Result")]
            ))
        
        # Layout centered on Boston
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_center_lat=42.3601,
            mapbox_center_lon=-71.0589,
            mapbox_zoom=11,
            margin=dict(l=0, r=0, t=0, b=0),
            height=500,
            showlegend=True,
            legend=dict(x=0, y=1, bgcolor="rgba(255,255,255,0.8)")
        )
        return fig
    
    initial_map = create_boston_map(facilities)
    return create_boston_map, initial_map


@app.cell
def __(mo):
    """Lat/Lon input for map click simulation."""
    mo.md("### Click Location Input")
    return ()


@app.cell
def __(mo):
    """Manual coordinate input."""
    lat_input = mo.ui.number(
        value=42.3601,
        start=42.0,
        stop=42.7,
        step=0.001,
        label="Latitude"
    )
    lon_input = mo.ui.number(
        value=-71.0589,
        start=-71.5,
        stop=-70.5,
        step=0.001,
        label="Longitude"
    )
    mo.hstack([lat_input, lon_input])
    return lat_input, lon_input


@app.cell
def __(mo):
    """Search button."""
    search_button = mo.ui.button(label="Find Nearest", kind="success")
    search_button
    return (search_button,)


@app.cell
def __(
    search_button,
    search_mode,
    lat_input,
    lon_input,
    facilities,
    time_picker,
    time_range,
    map_utils,
    mo
):
    """Handle search and find nearest facility/vehicle."""
    search_button  # Reactive dependency
    
    selected_lat = lat_input.value
    selected_lon = lon_input.value
    
    result = None
    result_table = None
    
    if search_mode.value == "Nearest Facility":
        if facilities:
            result = map_utils.find_nearest_facility(selected_lat, selected_lon, facilities)
            if result:
                result_table = mo.ui.table(
                    data=[{
                        "Stop ID": result["stop_id"],
                        "Stop Name": result["stop_name"],
                        "Latitude": f"{result['lat']:.4f}",
                        "Longitude": f"{result['lon']:.4f}",
                        "Distance (m)": f"{map_utils.haversine(selected_lat, selected_lon, result['lat'], result['lon']):.1f}"
                    }],
                    label="Nearest Facility"
                )
    else:
        # Vehicle search mode
        window_map = {"±5 min": 5, "±15 min": 15, "±60 min": 60}
        window_minutes = window_map.get(time_range.value, 15)
        
        # Placeholder for vehicle data - would fetch from lamp springboard
        result_table = mo.md(f"""
        **Vehicle Search Parameters:**
        - Location: ({selected_lat:.4f}, {selected_lon:.4f})
        - Time: {time_picker.value}
        - Window: ±{window_minutes} minutes
        
        *Vehicle data integration pending - will connect to lamp springboard*
        """)
    
    return result, result_table, selected_lat, selected_lon


@app.cell
def __(result_table, mo):
    """Display results table."""
    if result_table:
        mo.vstack([
            mo.md("### Search Results"),
            result_table
        ])
    else:
        mo.md("*Click 'Find Nearest' to search*")
    return ()


@app.cell
def __(
    create_boston_map,
    facilities,
    selected_lat,
    selected_lon,
    result,
    search_button
):
    """Update map with selected point and result."""
    search_button  # Reactive dependency
    
    selected_point = (selected_lat, selected_lon)
    result_point = None
    if result:
        result_point = {
            "lat": result["lat"],
            "lon": result["lon"],
            "name": result.get("stop_name", result.get("vehicle_id", "Result"))
        }
    
    updated_map = create_boston_map(facilities, selected_point, result_point)
    updated_map
    return result_point, selected_point, updated_map


@app.cell
def __(mo):
    """App header and instructions."""
    mo.md("""
    # Boston Map Trip Finder
    
    This app allows you to find the nearest MBTA facility or vehicle to a selected location.
    
    **Instructions:**
    1. Select search mode: "Nearest Facility" or "Nearest Vehicle"
    2. If searching for vehicles, configure the time and time range
    3. Enter latitude/longitude coordinates (or use defaults for Boston center)
    4. Click "Find Nearest" to search
    5. View results in the table and on the map
    
    ---
    """)
    return ()


if __name__ == "__main__":
    app.run()
