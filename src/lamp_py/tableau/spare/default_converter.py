import pyarrow
import pyarrow.dataset as pd

import polars as pl

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import S3Location
from typing import List


class PolarsDataFrameConverter(pl.DataFrame):
    def convert_to_tableau_flat_schema(self: pl.DataFrame, seperator: str = ".") -> pl.DataFrame:
        """
        Polars does not have nested struct string expansion on their roadmap - this implementation is adapted
        from user legout's solution: https://github.com/pola-rs/polars/issues/9613#issuecomment-1658376392
        """

        def _unnest_all(struct_columns: List[str]) -> pl.DataFrame:
            return self.with_columns(
                [
                    pl.col(col).struct.rename_fields(
                        [f"{col}{seperator}{field_name}" for field_name in self[col].struct.fields]
                    )
                    for col in struct_columns
                ]
            ).unnest(struct_columns)

        struct_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Struct)]
        list_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.List)]
        categorical_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Categorical)]
        u64_unsupported = [col for col in self.columns if isinstance(self[col].dtype, pl.UInt64)]
        # fold in the timezone stripping logic into here eventually...
        # dt_With_timezone = [col for col in self.columns if isinstance(self[col].dtype, pl.Datetime) and self[col].dtype.time_zone is not None]

        fully_flattened = not len(struct_columns) | len(list_columns) | len(categorical_columns) | len(u64_unsupported)
        while not fully_flattened:
            if len(struct_columns):
                self = _unnest_all(struct_columns=struct_columns)
            if len(list_columns):
                # don't expand lists - leave it as a string repr for now.
                # self = self.explode(list_columns).with_columns(pl.col(list_columns).cast(pl.String))
                # self['roles'].dtype.inner
                for col in list_columns:
                    # self = self.explode(columns=col)
                    try:
                        if isinstance(self[col].dtype.inner, pl.Categorical) | isinstance(self[col].dtype.inner, pl.String):  # type: ignore[attr-defined]
                            self = self.with_columns(pl.col(col).cast(pl.List(pl.String)).list.join(","))
                            continue
                        if isinstance(self[col].dtype.inner, pl.Struct):  # type: ignore[attr-defined]
                            self = self.with_columns(
                                pl.col(col).list.eval(pl.element().struct.json_encode()).list.join(",")
                            )
                            continue
                        if isinstance(self[col].dtype.inner, pl.String):  # type: ignore[attr-defined]
                            self = self.with_columns(pl.col(col).cast(pl.List(pl.String)).list.join(","))
                    except Exception as e:
                        print(e)
                        print("oops")
                    print(self[list_columns[0]].dtype)
            if len(categorical_columns):
                self = self.with_columns(pl.col(categorical_columns).cast(pl.String))
            if len(u64_unsupported):
                self = self.with_columns(pl.col(u64_unsupported).cast(pl.Int64))

            struct_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Struct)]
            list_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.List)]
            categorical_columns = [col for col in self.columns if isinstance(self[col].dtype, pl.Categorical)]
            u64_unsupported = [col for col in self.columns if isinstance(self[col].dtype, pl.UInt64)]

            fully_flattened = (
                not len(struct_columns) | len(list_columns) | len(categorical_columns) | len(u64_unsupported)
            )
        return self


def default_converter_from_s3(input_location: S3Location) -> pyarrow.Schema:
    """
    grabs parquet from s3, and does default conversion to make it tableau ready
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
        table = pl.from_arrow(first_batch).convert_to_tableau_flat_schema()

        return table.to_arrow().schema

    except pyarrow.lib.ArrowInvalid:  # pylint: disable=I1101
        print(f"{ds_paths[0]} is not pyarrow")
        raise
    except Exception as e:
        print(f"Caught an unexpected error: {e}")

    return first_batch
