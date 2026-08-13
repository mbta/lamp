from copy import deepcopy
import dataframely as dy
import polars as pl


def with_alias(column: dy.Column, new_alias: str) -> dy.Column:
    """Return the input column with a new alias."""
    new_column = deepcopy(column)
    new_column.alias = new_alias
    return new_column


def with_nullable(column: dy.Column, nullable: bool) -> dy.Column:
    """Return the input column and set its nullability."""
    new_column = deepcopy(column)
    new_column.nullable = nullable
    return new_column


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
                        name + "." + (v.alias if v.alias else k): with_nullable(v, col.nullable | v.nullable)
                        for k, v in col.inner.items()
                    }
                )
            )
        else:
            new_schema.update({name: col})
    return new_schema


def schema_as_frame(schema: dy.Schema) -> pl.DataFrame:
    """Print the schema as a Markdown table."""
    data = []
    for name, col in schema.columns().items():
        data.append(
            {
                "Column Name": "`" + name + "`",
                "Arrow type": col.pyarrow_dtype,
                "Is primary key": col.primary_key,
                "Is nullable": col.nullable,
                "Definition": col.metadata.get("definition", None) if col.metadata else None,
                "Constraints": "\n".join(
                    [
                        f"- `{k}`: `{v}`"
                        for k, v in col.as_dict(pl.col(name)).items()
                        if k not in ["column_type", "nullable", "primary_key", "metadata", "time_unit", "time_zone"]
                        and v is not None
                    ]
                ),
            }
        )
    return pl.DataFrame(data)
