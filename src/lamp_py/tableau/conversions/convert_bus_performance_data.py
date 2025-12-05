import polars as pl
from pyarrow import Table
import pyarrow


def convert_bus_recent_to_tableau_compatible_schema(
    input_schema: pyarrow.schema, overrides: dict | None = None, exclude: list | None = None
) -> pyarrow.schema:
    """
    generic converter for known types (dates, timezones, etc)
    overrides to correct the rest
    overrides_exclude -
    """
    # Create a map to store the modified fields that can't be auto-inferred
    auto_schema = []
    # Loop through the schema fields and apply changes
    for field in input_schema:

        if exclude is not None and field.name in exclude:
            continue
        if overrides is not None and field.name in overrides.keys():
            new_field = field.with_type(overrides[field.name])
            auto_schema.append(new_field)
            continue
        if isinstance(field.type, pyarrow.TimestampType):
            if field.type.tz is not None:
                # strip tz
                new_field = field.with_type(pyarrow.timestamp(unit=field.type.unit))
                auto_schema.append(new_field)
                continue

        # default - no changes, add the field again
        auto_schema.append(field)

    return pyarrow.schema(auto_schema)


def apply_bus_analysis_conversions(polars_df: pl.DataFrame) -> Table:
    """
    Function to apply final conversions to lamp data before outputting for tableau consumption
    """
    # Convert datetime to Eastern Time
    polars_df = polars_df.with_columns(
        pl.col("stop_arrival_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("stop_departure_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("gtfs_first_in_transit_dt")
        .dt.convert_time_zone(time_zone="America/New_York")
        .dt.replace_time_zone(None),
        pl.col("gtfs_last_in_transit_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("tm_actual_arrival_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("tm_actual_departure_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("gtfs_departure_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        pl.col("gtfs_arrival_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        # pl.col("plan_start_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
        # pl.col("plan_stop_departure_dt").dt.convert_time_zone(time_zone="America/New_York").dt.replace_time_zone(None),
    )

    # Convert seconds columns to be aligned with Eastern Time
    polars_df = polars_df.with_columns(
        (pl.col("gtfs_first_in_transit_dt") - pl.col("service_date"))
        .dt.total_seconds()
        .alias("gtfs_first_in_transit_seconds"),
        (pl.col("stop_arrival_dt") - pl.col("service_date")).dt.total_seconds().alias("stop_arrival_seconds"),
        (pl.col("stop_departure_dt") - pl.col("service_date")).dt.total_seconds().alias("stop_departure_seconds"),
    )

    polars_df = polars_df.with_columns(pl.col("service_date"))

    return polars_df.to_arrow()
