from typing import Type

import dataframely as dy
import msgspec
import msgspec.inspect as mi


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


TYPE_MAP: dict[Type[mi.Type], Type[dy.Column]] = {
    mi.StrType: dy.String,
    mi.IntType: dy.Int64,
    mi.FloatType: dy.Float64,
    mi.BoolType: dy.Bool,
    mi.BytesType: dy.Binary,
}

KWARG_MAP: dict[str, str] = {
    "pattern": "regex",
    "min_length": "min_length",
    "max_length": "max_length",
    "ge": "min",
    "le": "max",
    "gt": "min_exclusive",
    "lt": "max_exclusive",
}


def _handle_basic_type(field: mi.Type, nullable: bool, **kwargs) -> dy.Column | None:
    """Handle basic types like str, int, float, bool, bytes."""
    dy_type = TYPE_MAP.get(field.__class__)
    if not dy_type:
        return None

    annotations = {attr: getattr(field, attr) for attr in dir(field) if not attr.startswith("_")}
    constraints = {
        KWARG_MAP.get(ann, ann): getattr(field, ann)
        for ann in annotations
        if ann in KWARG_MAP and annotations[ann] is not None
    }
    return dy_type(nullable=nullable, **kwargs | constraints)


def _handle_union_type(field: mi.Type, _: bool, **kwargs) -> dy.Column | None:
    """Handle Optional types (Type | None)."""
    if not isinstance(field, mi.UnionType):
        return None
    if len(field.types) != 2 or mi.NoneType not in [t.__class__ for t in field.types]:
        return None

    non_none_type = next(t for t in field.types if t.__class__ is not mi.NoneType)
    return msgspec_to_dataframely_field(non_none_type, nullable=True, **kwargs)


def _handle_enum_type(field: mi.Type, nullable: bool, **kwargs) -> dy.Column | None:
    """Handle Enum types (IntEnum, StrEnum, etc)."""
    if not isinstance(field, mi.EnumType):
        return None

    if all(isinstance(m.value, int) for m in field.cls):
        return dy.Int64(is_in=[m.value for m in field.cls], nullable=nullable, **kwargs)
    return dy.Enum(categories=[m.value for m in field.cls], nullable=nullable, **kwargs)


def _handle_struct_type(field: mi.Type, nullable: bool, **kwargs) -> dy.Column | None:
    """Handle nested Struct types."""
    if not isinstance(field, mi.StructType):
        return None

    return dy.Struct(
        inner={subfield.name: msgspec_to_dataframely_field(subfield.type) for subfield in field.fields},
        nullable=nullable,
        **kwargs,
    )


def _handle_list_type(field: mi.Type, nullable: bool, **kwargs) -> dy.Column | None:
    """Handle List types."""
    if not isinstance(field, mi.ListType):
        return None

    return dy.List(inner=msgspec_to_dataframely_field(field.item_type), nullable=nullable, **kwargs)


def _handle_struct_meta(field: mi.Type, nullable: bool, **kwargs) -> dy.Column | None:
    """Handle StructMeta (direct struct class references)."""
    if not isinstance(field, msgspec.StructMeta):
        return None

    return _handle_struct_type(mi.type_info(field), nullable=nullable, **kwargs)


# Handler registry - order matters!
_TYPE_HANDLERS = [
    _handle_basic_type,
    _handle_union_type,
    _handle_enum_type,
    _handle_struct_type,
    _handle_list_type,
    _handle_struct_meta,
]


def msgspec_to_dataframely_field(
    field: msgspec.inspect.Type,
    nullable: bool = False,
    **kwargs: dict[str, int | str | bool | None],
) -> dy.Column:
    """
    Convert a msgspec Field or Type to a dataframely Column.

    Args:
        field: The msgspec Field from type_info or a msgspec Type
        nullable: Explicitly set nullability. If None, inferred from field type/requirements.
        **kwargs: Additional constraints to apply to the dataframely column (e.g. min_length, max_length, regex)

    Returns:
        A dataframely Column instance

    Examples:
        >>> import msgspec
        >>> class Example(msgspec.Struct):
        ...     name: str
        ...     age: int
        ...     score: float | None = None
        >>> info = msgspec.structs.fields(Example)
        >>> for field in info.fields:
        ...     df_field = msgspec_to_dataframely_field(field)
        ...     print(f"{field.name}: {df_field}")
    """
    for handler in _TYPE_HANDLERS:
        result = handler(field, nullable, **kwargs)
        if result is not None:
            return result

    raise NotImplementedError(f"No conversion implemented for msgspec type: {field.__class__}")


def struct_to_schema(
    struct_class: Type[msgspec.Struct],
    schema_name: str | None = None,
) -> Type[dy.Schema]:
    """
    Convert a msgspec.Struct class to a dataframely.Schema class.

    Args:
        struct_class: The msgspec.Struct class to convert
        schema_name: Optional name for the schema class. If None, uses struct_class name

    Returns:
        A dynamically created dataframely.Schema class

    Examples:
        >>> import msgspec
        >>> class Person(msgspec.Struct):
        ...     name: str
        ...     age: int
        ...     email: str | None = None
        >>> PersonSchema = struct_to_schema(Person)
    """
    info = mi.type_info(struct_class)

    if not isinstance(info, mi.StructType):
        raise TypeError(f"{struct_class} is not a msgspec.Struct")

    # Create dictionary of fields for the schema
    schema_fields = {}
    for field in info.fields:
        df_field = msgspec_to_dataframely_field(field.type)
        schema_fields[field.name] = df_field

    # Create schema class dynamically
    schema_name = schema_name or f"{struct_class.__name__}Schema"
    schema_class = type(schema_name, (dy.Schema,), schema_fields)

    return schema_class
