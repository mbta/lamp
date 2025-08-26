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


def unnest_all(self: pl.DataFrame, seperator="."):
    """
    Polars does not have nested struct string expansion on their roadmap - this implementation is adapted
    from user legout's solution: https://github.com/pola-rs/polars/issues/9613#issuecomment-1658376392
    """
    def _unnest_all(struct_columns):
        return self.with_columns(
            [
                pl.col(col).struct.rename_fields(
                    [
                        f"{col}{seperator}{field_name}"
                        for field_name in self[col].struct.fields
                    ]
                )
                for col in struct_columns
            ]
        ).unnest(struct_columns)
        
    struct_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Struct)]
    list_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.List)]
    categorical_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Categorical)]

    fully_flattened = not (len(struct_columns) | len(list_columns) | len(categorical_columns))
    while not fully_flattened: 
        if len(struct_columns):
            self = _unnest_all(struct_columns=struct_columns)
        if len(list_columns):
            for col in list_columns:
                self = self.explode(columns=col)
        if len(categorical_columns):
            self = self.with_columns(pl.col(categorical_columns).cast(pl.String))

        struct_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Struct)]
        list_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.List)]
        categorical_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Categorical)]
        fully_flattened = not (len(struct_columns) | len(list_columns) | len(categorical_columns))
    return self

pl.DataFrame.unnest_all = unnest_all

def default_converter_from_s3(input_location: S3Location) -> pl.DataFrame | None:
    """
    Apply transforms to spare vehicle.parquet
    """

    # grab a single file in the dataset to get the schema
    s3_uris = file_list_from_s3(bucket_name=input_location.bucket, file_prefix=input_location.prefix, max_list_size=1)
    ds_paths = [s.replace("s3://", "") for s in s3_uris]
    try:
        ds = pd.dataset(
            ds_paths,
            format="parquet",
            filesystem=pyarrow.fs.S3FileSystem(),
        )
        first_batch = next(ds.to_batches(batch_size=10, batch_readahead=0, fragment_readahead=0))
        table = pl.from_arrow(first_batch)

        table = default_converter(table)

        return table.to_arrow().schema
        
    except pyarrow.lib.ArrowInvalid:
        print(f"{ds_paths[0]} is not pyarrow")
        raise
    except Exception as e:
        print(f"Caught an unexpected error: {e}")




def default_converter(table: pl.DataFrame) -> pl.DataFrame:
    """
    Apply transforms to spare vehicle.parquet
    """

    return table.unnest_all()

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
