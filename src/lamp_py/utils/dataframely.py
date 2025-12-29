import dataframely as dy


def with_alias(column: dy.Column, new_alias: str) -> dy.Column:
    """Return the input column with a new alias."""
    column.alias = new_alias

    return column


def unnest_columns(columns: dict[str, dy.Column]) -> dict[str, dy.Column]:
    """Return a schema without any lists or structs named using `.` to delineate former nested structures."""
    new_schema = {}
    for name, col in columns.items():
        if isinstance(col, dy.List):
            alias = col.inner.alias if col.inner.alias else name
            new_schema.update(unnest_columns({alias: with_alias(col.inner, alias)}))
        elif isinstance(col, dy.Struct):
            new_schema.update(
                unnest_columns(
                    {
                        name
                        + "."
                        + (v.alias if v.alias else k): with_alias(v, name + "." + (v.alias if v.alias else k))
                        for k, v in col.inner.items()
                    }
                )
            )
        else:
            new_schema.update({name: col})
    return new_schema
