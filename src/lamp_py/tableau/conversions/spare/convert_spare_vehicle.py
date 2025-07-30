import pyarrow
import polars as pl
from lamp_py.ingestion.utils import explode_table_column, flatten_schema, flatten_table_schema


def flatten_spare_vehicle(table: pyarrow.Table) -> pyarrow.Table:
    table = table.drop(["metadata.providerId", "metadata.owner", "emissionsRate", "createdAt", "updatedAt"])
    df = pl.from_arrow(table)
    accessibilty_rows = flatten_table_schema(
        explode_table_column(flatten_table_schema(table), "accessibilityFeatures")
    )  # .drop(['accessibilityFeatures'])

    return pl.concat([df, pl.from_arrow(accessibilty_rows)], how="align").to_arrow().drop("accessibilityFeatures")
