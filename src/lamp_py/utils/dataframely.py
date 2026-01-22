import dataframely as dy


def with_alias(column: dy.Column, new_alias: str) -> dy.Column:
    """Return the input column with a new alias."""
    column.alias = new_alias

    return column


def with_nullable(column: dy.Column, nullable: bool) -> dy.Column:
    """Return the input column and set its nullability."""
    column.nullable = nullable
    return column


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


def has_metadata(column: dy.Column, key: str) -> bool:
    """Check if a column has specific metadata key."""
    if column.metadata is None:
        return False

    return key in column.metadata.keys()
