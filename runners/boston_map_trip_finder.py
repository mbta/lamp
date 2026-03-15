"""
Boston Map Trip Finder - Marimo Notebook
Interactive map for finding nearest MBTA facilities or vehicles.
Use the sliders to select a location and find the nearest stop.
"""

import marimo

__generated_with = "0.14.17"
app = marimo.App(width="full")


@app.cell
def _():
    """Imports and dependencies."""
    import marimo as mo
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    import map_utils
    return datetime, go, map_utils, mo


@app.cell
def _(mo):
    """App header and instructions."""
    mo.md("""
    # Boston Map Trip Finder

    **Use the sliders** to select a location. The map shows your selected point and the nearest MBTA station.

    ---
    """)
    return


@app.cell
def _(mo):
    """Search mode radio buttons."""
    search_mode = mo.ui.radio(
        options=["Nearest Facility", "Nearest Vehicle"],
        value="Nearest Facility",
        label="Search Mode"
    )
    search_mode
    return (search_mode,)


@app.cell
def _(datetime, mo):
    """Time picker for vehicle search."""
    time_picker = mo.ui.datetime(
        value=datetime.now(),
        label="Select Time"
    )
    return (time_picker,)


@app.cell
def _(mo):
    """Time range selector for vehicle search."""
    time_range = mo.ui.radio(
        options=["±5 min", "±15 min", "±60 min"],
        value="±15 min",
        label="Time Range"
    )
    return (time_range,)


@app.cell
def _(mo, search_mode, time_picker, time_range):
    """Conditionally show time controls only for vehicle search."""
    if search_mode.value == "Nearest Vehicle":
        mo.vstack([
            mo.md("### Vehicle Search Options"),
            time_picker,
            time_range
        ])
    return


@app.cell
def _(map_utils):
    """Fetch stops from MBTA V3 API - only parent stations."""
    MBTA_V3_STOPS_URL = "https://api-v3.mbta.com/stops"
    try:
        all_stops = map_utils.fetch_stops_v3(MBTA_V3_STOPS_URL)
        # Filter to only parent stations (location_type=1)
        facilities = [s for s in all_stops if s.get("location_type") == "1"]
        facilities_status = f"Loaded {len(facilities)} stations (from {len(all_stops)} total stops)"
    except Exception as e:
        facilities = []
        facilities_status = f"Error loading stops: {e}"
    return facilities, facilities_status


@app.cell
def _(facilities_status, mo):
    """Display facilities loading status."""
    mo.md(f"*{facilities_status}*")
    return


@app.cell
def _(mo):
    """Latitude slider - Boston area bounds."""
    # Boston area: roughly 42.2 to 42.5 latitude
    lat_slider = mo.ui.slider(
        start=42.20,
        stop=42.50,
        step=0.001,
        value=42.3601,
        label="Latitude",
        show_value=True
    )
    return (lat_slider,)


@app.cell
def _(mo):
    """Longitude slider - Boston area bounds."""
    # Boston area: roughly -71.2 to -70.9 longitude
    lon_slider = mo.ui.slider(
        start=-71.20,
        stop=-70.90,
        step=0.001,
        value=-71.0589,
        label="Longitude",
        show_value=True
    )
    return (lon_slider,)


@app.cell
def _(lat_slider, lon_slider, mo):
    """Display coordinate controls."""
    mo.vstack([
        mo.md("### Select Location"),
        lat_slider,
        lon_slider,
        mo.md(f"**Selected:** ({lat_slider.value:.4f}, {lon_slider.value:.4f})")
    ])
    return


@app.cell
def _(facilities, go, lat_slider, lon_slider, map_utils):
    """Create Boston map centered on selected point."""
    
    sel_lat = lat_slider.value
    sel_lon = lon_slider.value
    
    # Find nearest facility
    nearest = map_utils.find_nearest_facility(sel_lat, sel_lon, facilities) if facilities else None
    
    # Create figure
    fig = go.Figure()
    
    # Add station markers
    if facilities:
        stop_data = map_utils.prepare_facility_plotly_data(facilities)
        fig.add_trace(go.Scattermapbox(
            lat=stop_data["lat"],
            lon=stop_data["lon"],
            text=stop_data["text"],
            customdata=stop_data["customdata"],
            mode="markers",
            marker=dict(size=8, color="blue", opacity=0.6),
            name="Stations",
            hovertemplate="<b>%{text}</b><br>ID: %{customdata}<extra></extra>"
        ))
    
    # Add selected point (center crosshair)
    fig.add_trace(go.Scattermapbox(
        lat=[sel_lat],
        lon=[sel_lon],
        mode="markers",
        marker=dict(size=16, color="red", symbol="circle"),
        name="Selected",
        hovertemplate="<b>Selected</b><br>Lat: %{lat:.4f}<br>Lon: %{lon:.4f}<extra></extra>"
    ))
    
    # Add nearest station marker
    if nearest:
        fig.add_trace(go.Scattermapbox(
            lat=[nearest["lat"]],
            lon=[nearest["lon"]],
            mode="markers",
            marker=dict(size=20, color="green"),
            name="Nearest",
            text=[nearest["stop_name"]],
            hovertemplate="<b>%{text}</b><extra></extra>"
        ))
    
    # Layout - centered on selected point, limited to Boston area
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=sel_lat, lon=sel_lon),
            zoom=12,
            # Bounds limit zooming out past Boston
            bounds=dict(
                west=-71.25,
                east=-70.85,
                south=42.15,
                north=42.55
            )
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=500,
        showlegend=True,
        legend=dict(x=0, y=1, bgcolor="rgba(255,255,255,0.8)")
    )
    
    fig
    return nearest, sel_lat, sel_lon


@app.cell
def _(map_utils, mo, nearest, sel_lat, sel_lon):
    """Display results table."""
    if nearest:
        distance = map_utils.haversine(sel_lat, sel_lon, nearest["lat"], nearest["lon"])
        result_display = mo.vstack([
            mo.md("### Nearest Station"),
            mo.ui.table(
                data=[{
                    "Stop ID": nearest["stop_id"],
                    "Stop Name": nearest["stop_name"],
                    "Latitude": f"{nearest['lat']:.5f}",
                    "Longitude": f"{nearest['lon']:.5f}",
                    "Distance": f"{distance:.0f} m"
                }]
            )
        ])
    else:
        result_display = mo.md("*Loading stops...*")

    result_display
    return


@app.cell
def _(mo, search_mode, sel_lat, sel_lon, time_picker, time_range):
    """Show vehicle search parameters when in vehicle mode."""
    if search_mode.value == "Nearest Vehicle":
        window_map = {"±5 min": 5, "±15 min": 15, "±60 min": 60}
        window_minutes = window_map.get(time_range.value, 15)

        mo.md(f"""
        ### Vehicle Search Parameters
        - **Location:** ({sel_lat:.5f}, {sel_lon:.5f})
        - **Time:** {time_picker.value}
        - **Window:** ±{window_minutes} minutes

        *Vehicle data integration pending - will connect to lamp springboard*
        """)
    return


if __name__ == "__main__":
    app.run()
