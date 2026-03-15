## Plan: Configurable Marimo Map App with Facility/Vehicle Search

This plan extends the Marimo notebook to include radio buttons for search mode (nearest facility or nearest vehicle), and a configurable time range selector. When "nearest vehicle" is selected, results are shown for the chosen time window (±5, ±15, ±60 minutes from selected time).

**Steps**
1. **Update Utility Module ([runners/map_utils.py](runners/map_utils.py))**
   - Add/extend functions to:
     - Search for nearest vehicle at a location and time window (using lamp springboard or GTFS-RT data).
     - Support time range filtering (±5, ±15, ±60 minutes).
   - Ensure facility search logic remains available.

2. **Enhance Tests ([tests/test_map_utils.py](tests/test_map_utils.py))**
   - Add tests for vehicle search logic, including time window filtering.
   - Validate radio button logic and output for both modes.

3. **Notebook UI ([runners/boston_map_trip_finder.marimo](runners/boston_map_trip_finder.marimo))**
   - Add radio buttons for "Nearest Facility" and "Nearest Vehicle".
   - When "Nearest Vehicle" is selected:
     - Show time range options (±5, ±15, ±60 minutes).
     - Enable time picker.
   - On map click, call appropriate utility function based on mode and time range.
   - Display results in a table (facility or vehicle details).

4. **Documentation**
   - Update notebook intro and [README.md](README.md) to explain search modes and time range options.

**Verification**
- Run tests for both facility and vehicle search utilities.
- Launch notebook and verify:
  - Radio buttons switch modes correctly.
  - Time range options appear and filter results as expected.
  - Map click triggers correct search and updates table.

**Decisions**
- Radio buttons for mode selection, time range options for vehicle search.
- All logic in [runners/map_utils.py](runners/map_utils.py) for testability.
- Notebook UI adapts based on selected mode.

Ready for review or further refinement before implementation.
