from copy import copy
from typing import Type

import dataframely as dy
import polars as pl

from lamp_py.runtime_utils.process_logger import ProcessLogger


def with_alias(column: dy.Column, new_alias: str) -> dy.Column:
    """Return the input column with a new alias."""
    new_col = copy(column)
    new_col.alias = new_alias
    return new_col


def with_nullable(column: dy.Column, nullable: bool) -> dy.Column:
    """Return the input column and set its nullability."""
    new_col = copy(column)
    new_col.nullable = nullable
    return new_col


def unnest_columns(columns: dict[str, dy.Column]) -> dict[str, dy.Column]:
    """Return a schema without any lists or structs named using `.` to delineate former nested structures. Does not support aliases defined inside dy.Column types."""
    new_schema = {}
    for name, col in columns.items():
        if isinstance(col, dy.List):
            nullability = col.nullable | col.inner.nullable
            alias = name + ("." + col.inner.alias if col.inner.alias else "")
            new_schema.update(unnest_columns({alias: with_nullable(with_alias(col.inner, alias), nullability)}))
        elif isinstance(col, dy.Struct):
            new_schema.update(
                unnest_columns(
                    {
                        name
                        + "."
                        + (v.alias or k): with_nullable(
                            with_alias(v, name + "." + (v.alias or k)), col.nullable | v.nullable
                        )
                        for k, v in col.inner.items()
                    }
                )
            )
        else:
            new_schema.update({name: col})
    return new_schema


def has_metadata(column: dy.Column, key: str) -> bool:
    """Check if a column has specific metadata key."""
    if column.metadata is None:
        return False

    return key in column.metadata.keys()


def extract_pii_columns(schema: Type[dy.Schema]) -> list[dy.Column]:
    """Extract columns that have PII metadata."""
    pii_columns = [col for col in schema.columns().values() if has_metadata(col, "pii_roles")]
    return pii_columns


def drop_pii_columns(df: pl.DataFrame, schema: Type[dy.Schema]) -> pl.DataFrame:
    """Drop PII columns from a dataframely-typed DataFrame."""
    process_logger = ProcessLogger("remove_pii_columns")
    process_logger.log_start()
    pii_columns = extract_pii_columns(schema)
    if pii_columns:
        process_logger.add_metadata(
            pii_fields=", ".join([col.name for col in pii_columns]),
        )

        pii_keys = set(col.name for col in pii_columns).intersection(schema.primary_key())
        assert not pii_keys, f"Splitting would remove primary keys {pii_keys} from table."

    non_pii_df = df.drop(col.name for col in pii_columns)
    return non_pii_df
