from typing import Callable
import pyarrow
import pyarrow.dataset as pd

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import S3Location


def get_default_tableau_schema_from_s3(
    input_location: S3Location,
    preprocess: Callable[[pyarrow.Table], pyarrow.Table] | None = None,
    overrides: dict | None = None,
    excludes: dict | None = None,
) -> pyarrow.schema:
    """
    This function automatically applies a set of schema transformations to make a table tableau compatible
    There are some datatypes that are supported that are different than the pyarrow supported set.

    Function reads from input_location to get a parquet file, and reads the schema from it without loading
    The schema can be pre-processed with a method, or type changed or excluded with overrides

    Parameters
    ----------
    input_location : location to read a dataset from
    preprocess : [Optional] function to transform a schema. The function signature is Table, but the retur
    overrides: dict of {"column_name": "new_type"} to apply to schema
    excludes: dict of {"column_name": "exclude_reason_string"} to apply to schema

    Returns
    -------
    pyarrow.schema that is valid for upload to tableau
    """

    # grab a single file in the dataset to get the schema
    s3_uris = file_list_from_s3(bucket_name=input_location.bucket, file_prefix=input_location.prefix, max_list_size=1)
    ds_paths = [s.replace("s3://", "") for s in s3_uris]

    ds = pd.dataset(
        ds_paths,
        format="parquet",
        filesystem=pyarrow.fs.S3FileSystem(),
    )
    if preprocess is not None:
        ds = preprocess(ds.schema.empty_table())

    return convert_to_tableau_compatible_schema(ds.schema, overrides, excludes)


def convert_to_tableau_compatible_schema(
    input_schema: pyarrow.schema,
    overrides: dict | None = None,
    excludes: dict | None = None,
) -> pyarrow.schema:
    """
    This function automatically applies a set of schema transformations to make a table tableau compatible
    There are some datatypes that are supported that are different than the pyarrow supported set.

    Generic converter for known tableau types (dates, timezones, etc)

    Parameters
    ----------
    input_schema : pyarrow schema to transform
    overrides: dict of {"column_name": "new_type"} to apply to schema
    excludes: dict of {"column_name": "exclude_reason_string"} to apply to schema

    Returns
    -------
    pyarrow.schema that is valid for upload to tableau

    """

    # Create a map to store the modified fields that can't be auto-inferred
    auto_schema = []
    # Loop through the schema fields and apply changes
    for field in input_schema:
        # print(field)
        if excludes is not None and field.name in excludes:
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
