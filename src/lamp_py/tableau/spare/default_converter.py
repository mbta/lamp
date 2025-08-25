from typing import OrderedDict
import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as pd

import polars as pl
from polars import String, UInt32, UInt64, List, Struct, Boolean, Categorical

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.ingestion.utils import explode_table_column, flatten_table_schema
from lamp_py.runtime_utils.remote_files import S3Location
from lamp_py.runtime_utils.remote_files_spare import (
    springboard_spare_vehicles,
    tableau_spare_vehicles,
)
from lamp_py.tableau.conversions.convert_types import get_default_tableau_schema_from_s3
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob


def default_converter_from_s3(input_location: S3Location) -> pl.DataFrame:
    """
    Apply transforms to spare vehicle.parquet
    """

    # grab a single file in the dataset to get the schema
    s3_uris = file_list_from_s3(bucket_name=input_location.bucket, file_prefix=input_location.prefix, max_list_size=1)
    ds_paths = [s.replace("s3://", "") for s in s3_uris]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=pyarrow.fs.S3FileSystem(),
    )
    table = pl.from_arrow(ds.to_batches(batch_size=1))

    table = default_converter(table)

    ret = table.to_arrow().schema
    return ret 

def default_converter(table: pl.DataFrame) -> pl.DataFrame:
    """
    Apply transforms to spare vehicle.parquet
    """

    # need to recurse here..
    for col_name,col_type, in table.collect_schema().items():
        if isinstance(col_type, pl.List):
            table = table.explode(columns=col_name)
            print(f"explode {col_name}")
            col_type = table[col_name].dtype
            
        if isinstance(col_type, pl.Struct):
            table = table.unnest(col_name)
            print(f"unnest {col_name}")

        if isinstance(col_type, pl.Categorical):
            table = table.with_columns(pl.col(col_name).cast(pl.String))
            print(f"decategorize {col_name}")
    return table 

    # table = table.drop(["metadata.providerId", "metadata.owner", "emissionsRate", "createdAt", "updatedAt"])


# def explode_all(table: pyarrow.Table, columns: list):

#     for col in columns:
#         df = pl.from_arrow(table)
#         exploded_rows = flatten_table_schema(
#             explode_table_column(flatten_table_schema(table), col)
#         )
#         return pl.concat([df, pl.from_arrow(exploded_rows)], how="align").to_arrow().drop(col)


if __name__ == "__main__":

    output_schema = get_default_tableau_schema_from_s3(
        springboard_spare_vehicles,
        preprocess=default_converter,
    )
    print(output_schema)

    vehicle = pl.read_parquet("s3://mbta-ctd-dataplatform-staging-springboard/spare/vehicles.parquet")
    # breakpoint()

    input = OrderedDict(
        [
            ("id", String),
            ("identifier", String),
            ("ownerUserId", String),
            ("ownerType", String),
            ("make", String),
            ("model", String),
            ("color", String),
            ("licensePlate", String),
            ("passengerSeats", UInt32),
            (
                "accessibilityFeatures",
                List(Struct({"type": String, "count": UInt32, "seatCost": UInt32, "requireFirstInLastOut": Boolean})),
            ),
            ("status", String),
            ("metadata", String),
            ("metadata.vin", String),
            ("metadata.providerId", Categorical(String)),
            ("metadata.owner", Categorical(String)),
            ("metadata.year", UInt32),
            ("metadata.comments", String),
            ("emissionsRate", UInt64),
            ("vehicleTypeId", String),
            ("capacityType", String),
            ("createdAt", UInt64),
            ("updatedAt", UInt64),
            ("photoUrl", String),
        ]
    )
    input_df = pl.Schema(schema=input).to_frame()
    output_df = pl.Schema(schema=output_schema).to_frame()
    assert vehicle.schema == input_df.schema
    print("done")

