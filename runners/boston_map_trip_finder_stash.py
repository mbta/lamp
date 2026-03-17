"""
Boston Map Trip Finder - Marimo Notebook
Interactive map for finding nearest MBTA facilities or vehicles.
Drag a box on the map to select stations in that area.
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

    **Use the sliders** to define a bounding box. Stations inside the box are highlighted in green and listed in the table below.

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
    """Bounding box sliders for selection area."""
    # Default to a small area in downtown Boston
    lat_min = mo.ui.slider(start=42.20, stop=42.50, step=0.005, value=42.34, label="South Bound", show_value=True)
    lat_max = mo.ui.slider(start=42.20, stop=42.50, step=0.005, value=42.38, label="North Bound", show_value=True)
    lon_min = mo.ui.slider(start=-71.20, stop=-70.90, step=0.005, value=-71.10, label="West Bound", show_value=True)
    lon_max = mo.ui.slider(start=-71.20, stop=-70.90, step=0.005, value=-71.02, label="East Bound", show_value=True)
    return lat_max, lat_min, lon_max, lon_min


@app.cell
def _(lat_max, lat_min, lon_max, lon_min, mo):
    """Display bounding box controls."""
    mo.vstack([
        mo.md("### Selection Area"),
        mo.hstack([lat_min, lat_max], gap=2),
        mo.hstack([lon_min, lon_max], gap=2),
    ])
    return


@app.cell
def _(facilities, go, lat_max, lat_min, lon_max, lon_min):
    """Create Boston map showing selection box."""
    
    # Filter stations in bounding box
    selected_facilities = [
        f for f in facilities
        if lat_min.value <= f["lat"] <= lat_max.value
        and lon_min.value <= f["lon"] <= lon_max.value
    ]
    
    other_facilities = [
        f for f in facilities
        if not (lat_min.value <= f["lat"] <= lat_max.value
                and lon_min.value <= f["lon"] <= lon_max.value)
    ]
    
    fig = go.Figure()
    
    # Add unselected stations (faded)
    if other_facilities:
        fig.add_trace(go.Scattermapbox(
            lat=[f["lat"] for f in other_facilities],
            lon=[f["lon"] for f in other_facilities],
            text=[f["stop_name"] for f in other_facilities],
            mode="markers",
            marker=dict(size=8, color="gray", opacity=0.3),
            name="Other Stations",
            hovertemplate="<b>%{text}</b><extra></extra>"
        ))
    
    # Add selected stations (highlighted)
    if selected_facilities:
        fig.add_trace(go.Scattermapbox(
            lat=[f["lat"] for f in selected_facilities],
            lon=[f["lon"] for f in selected_facilities],
            text=[f["stop_name"] for f in selected_facilities],
            mode="markers",
            marker=dict(size=12, color="green", opacity=0.9),
            name="Selected Stations",
            hovertemplate="<b>%{text}</b> (SELECTED)<extra></extra>"
        ))
    
    # Draw selection rectangle
    box_lats = [lat_min.value, lat_min.value, lat_max.value, lat_max.value, lat_min.value]
    box_lons = [lon_min.value, lon_max.value, lon_max.value, lon_min.value, lon_min.value]
    fig.add_trace(go.Scattermapbox(
        lat=box_lats,
        lon=box_lons,
        mode="lines",
        line=dict(width=3, color="red"),
        name="Selection Box",
        hoverinfo="skip"
    ))
    
    # Layout
    center_lat = (lat_min.value + lat_max.value) / 2
    center_lon = (lon_min.value + lon_max.value) / 2
    
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=12,
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
    return (selected_facilities,)


@app.cell
def _(selected_facilities, mo):
    """Display selected stations table."""
    
    if selected_facilities:
        table_data = [{
            "Stop ID": f["stop_id"],
            "Stop Name": f["stop_name"],
            "Latitude": f"{f['lat']:.5f}",
            "Longitude": f"{f['lon']:.5f}"
        } for f in selected_facilities]
        
        result = mo.vstack([
            mo.md(f"### Selected Stations ({len(selected_facilities)})"),
            mo.ui.table(data=table_data)
        ])
    else:
        result = mo.md("*No stations in selection area. Adjust the sliders to select a region.*")
    
    result
    return


@app.cell
def _(mo, search_mode, time_picker, time_range):
    """Show vehicle search parameters when in vehicle mode."""
    if search_mode.value == "Nearest Vehicle":
        window_map = {"±5 min": 5, "±15 min": 15, "±60 min": 60}
        window_minutes = window_map.get(time_range.value, 15)

        mo.md(f"""
        ### Vehicle Search Parameters
        - **Time:** {time_picker.value}
        - **Window:** ±{window_minutes} minutes

        *Select stations on the map, then vehicle data will be fetched for those locations.*
        """)
    return


if __name__ == "__main__":
    app.run()
