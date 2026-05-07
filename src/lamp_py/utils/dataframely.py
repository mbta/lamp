import dataframely as dy


def unnest_columns(columns: dict[str, dy.Column]) -> dict[str, dy.Column]:
    """Return a schema without any lists or structs named using `.` to delineate former nested structures. Does not support aliases defined inside dy.Column types."""
    new_schema = {}
    for name, col in columns.items():
        if isinstance(col, dy.List):
            nullability = col.nullable | col.inner.nullable
            alias = name + ("." + col.inner.alias if col.inner.alias else "")
            new_schema.update(unnest_columns({alias: col.inner.with_properties(nullable=nullability, alias=alias)}))
        elif isinstance(col, dy.Struct):
            new_schema.update(
                unnest_columns(
                    {
                        name + "." + (v.alias if v.alias else k): v.with_nullable(nullable=col.nullable | v.nullable)
                        for k, v in col.inner.items()
                    }
                )
            )
        else:
            new_schema.update({name: col})
    return new_schema
