"""
Boston Map Trip Finder - Marimo Notebook
Interactive map for finding nearest MBTA facilities or vehicles.
Double-click on the map to select a location and find the nearest stop.
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

    **Double-click on the map** to select a location and find the nearest MBTA stop.

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
    """Fetch stops from MBTA V3 API."""
    MBTA_V3_STOPS_URL = "https://api-v3.mbta.com/stops"
    try:
        facilities = map_utils.fetch_stops_v3(MBTA_V3_STOPS_URL)
        facilities_status = f"Loaded {len(facilities)} stops from MBTA V3 API"
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
    """State for selected coordinates."""
    selected_coords = mo.state({"lat": 42.3601, "lon": -71.0589})
    return (selected_coords,)


@app.cell
def _(facilities, go, map_utils, mo, selected_coords):
    """Create interactive Boston map with click handling."""

    def create_map_figure(stops, selected_lat, selected_lon, nearest_stop=None):
        fig = go.Figure()

        # Add stop markers
        if stops:
            stop_data = map_utils.prepare_facility_plotly_data(stops)
            fig.add_trace(go.Scattermapbox(
                lat=stop_data["lat"],
                lon=stop_data["lon"],
                text=stop_data["text"],
                customdata=stop_data["customdata"],
                mode="markers",
                marker=dict(size=6, color="blue", opacity=0.5),
                name="Stops",
                hovertemplate="<b>%{text}</b><br>ID: %{customdata}<extra></extra>"
            ))

        # Add selected point marker
        fig.add_trace(go.Scattermapbox(
            lat=[selected_lat],
            lon=[selected_lon],
            mode="markers",
            marker=dict(size=14, color="red"),
            name="Selected",
            hovertemplate="<b>Selected Point</b><br>Lat: %{lat:.4f}<br>Lon: %{lon:.4f}<extra></extra>"
        ))

        # Add nearest result marker
        if nearest_stop:
            fig.add_trace(go.Scattermapbox(
                lat=[nearest_stop["lat"]],
                lon=[nearest_stop["lon"]],
                mode="markers",
                marker=dict(size=16, color="green"),
                name="Nearest",
                text=[nearest_stop["stop_name"]],
                hovertemplate="<b>%{text}</b><extra></extra>"
            ))

        # Layout
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_center_lat=selected_lat,
            mapbox_center_lon=selected_lon,
            mapbox_zoom=13,
            margin=dict(l=0, r=0, t=0, b=0),
            height=500,
            showlegend=False,
            clickmode="event"
        )
        return fig

    breakpoint()
    # Get current selection
    coords = selected_coords.value
    sel_lat, sel_lon = coords["lat"], coords["lon"]

    # Find nearest facility
    nearest = map_utils.find_nearest_facility(sel_lat, sel_lon, facilities) if facilities else None

    # Create the map
    map_figure = create_map_figure(facilities, sel_lat, sel_lon, nearest)

    # Wrap in plotly UI element for click events
    interactive_map = mo.ui.plotly(map_figure)
    interactive_map

    return interactive_map, nearest, sel_lat, sel_lon


@app.cell
def _(interactive_map, selected_coords):
    """Handle map clicks to update selected coordinates."""
    # Check for click data from the plotly figure
    click_data = interactive_map.value

    if click_data and "points" in click_data:
        points = click_data["points"]
        if points and len(points) > 0:
            point = points[0]
            if "lat" in point and "lon" in point:
                new_lat = point["lat"]
                new_lon = point["lon"]
                # Update state
                selected_coords.set_value({"lat": new_lat, "lon": new_lon})

    return


@app.cell
def _(map_utils, mo, nearest, sel_lat, sel_lon):
    """Display results table."""
    if nearest:
        distance = map_utils.haversine(sel_lat, sel_lon, nearest["lat"], nearest["lon"])
        result_display = mo.vstack([
            mo.md("### Nearest Stop"),
            mo.ui.table(
                data=[{
                    "Stop ID": nearest["stop_id"],
                    "Stop Name": nearest["stop_name"],
                    "Latitude": f"{nearest['lat']:.5f}",
                    "Longitude": f"{nearest['lon']:.5f}",
                    "Distance": f"{distance:.0f} m"
                }]
            ),
            mo.md(f"**Selected Point:** ({sel_lat:.5f}, {sel_lon:.5f})")
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
