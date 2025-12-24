import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return


@app.cell
def _():
    from datetime import date
    return


@app.cell
def _():
    from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob

    return


@app.cell
def _():
    # August 24, 2025 
    # December 13, 2025
    # 16 weeks
    return


@app.cell
def _():
    # first - regenerate all of these - do they have the current schema?
    return


@app.cell
def _(LAMP, S3Location, S3_PUBLIC, os):
    tableau_bus_fall_rating = S3Location(bucket=S3_PUBLIC, prefix=os.path.join(LAMP, "bus_vehicle_events"), version="1.2")
    return


app._unparsable_cell(
    r"""
    HyperGtfsRtVehiclePositions = FilteredHyperJob(
        remote_input_location=bus_events,
        remote_output_location=tableau_bus_fall_rating,
        start_date=date(2025, 8,24)
        end_date=date(2025,12,13)
        processed_schema=convert_gtfs_rt_vehicle_position.LightRailTerminalVehiclePositions.to_pyarrow_schema(),
        dataframe_filter=convert_gtfs_rt_vehicle_position.lrtp,
        parquet_filter=FilterBankRtVehiclePositions.ParquetFilter.light_rail,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
