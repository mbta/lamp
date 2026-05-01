import marimo

__generated_with = "0.16.5"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import dataframely as dy
    return (pl,)


@app.cell
def _(pl):
    vehicle_positions = pl.scan_parquet("s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_VehiclePositions_LR_60_day.parquet")
    vehicle_positions.head(100).collect()
    return


@app.cell
def _(pl):
    trip_updates = pl.scan_parquet("s3://mbta-ctd-dataplatform-archive/lamp/tableau/devgreen-gtfs-rt/LAMP_DEVGREEN_RT_TripUpdates_LR_60_day.parquet")
    trip_updates.head(100).collect()
    return


if __name__ == "__main__":
    app.run()
