from contextlib import nullcontext
from enum import IntEnum, Enum, StrEnum, Flag
from typing import Type, Annotated, Any

import dataframely as dy
import msgspec
import polars as pl
import pytest

from polyfactory.factories.msgspec_factory import MsgspecFactory

from lamp_py.utils.typing import (
    msgspec_to_dataframely_field,
    unnest_columns,
    with_alias,
    with_nullable,
    struct_to_schema,
)


# new name
# new name with reserved characters
@pytest.mark.parametrize(
    ["alias"],
    [
        ("abc",),
        ("1",),
        ("`.''",),
    ],
    ids=[
        "letters",
        "numbers",
        "normally-reserved",
    ],
)
def test_with_alias(alias: str) -> None:
    """It replaces the alias with the specified string."""
    new_col = with_alias(dy.Struct(inner={"test": dy.String()}), alias)

    assert alias == new_col.alias


@pytest.mark.parametrize(
    ["dtype"],
    [
        (dy.Binary,),
        (dy.Bool,),
        (dy.Categorical,),
        (dy.Date,),
        (dy.Datetime,),
        (dy.Decimal,),
        (dy.Float,),
        (dy.Float32,),
        (dy.Float64,),
        (dy.Integer,),
        (dy.Int16,),
        (dy.Int32,),
        (dy.Int64,),
        (dy.Int8,),
        (dy.Object,),
        (dy.String,),
        (dy.Time,),
        (dy.Int64,),
        (dy.Int64,),
        (dy.Int64,),
        (dy.Int64,),
    ],
)
@pytest.mark.parametrize(
    ["input_nullability"],
    [
        (True,),
        (False,),
    ],
)
@pytest.mark.parametrize(
    ["desired_nullability"],
    [
        (True,),
        (False,),
    ],
)
def test_with_nullable(dtype: Type[dy.Column], input_nullability: bool, desired_nullability: bool) -> None:
    """It always sets the specified nullability."""
    new_column = dtype(nullable=input_nullability)
    assert desired_nullability == with_nullable(new_column, desired_nullability).nullable


@pytest.mark.parametrize(
    ["columns", "expected_output"],
    [
        ({"col1": dy.String()}, {"col1": dy.String()}),
        ({"col1": dy.List(dy.String())}, {"col1": dy.String()}),
        ({"col1": dy.List(dy.Int16(alias="number"))}, {"col1.number": dy.Int16()}),
        ({"col1": dy.List(dy.List(dy.String(alias="nested")))}, {"col1.nested": dy.String()}),
        (
            {"col1": dy.Struct({"not_real_alias": dy.String(alias="real_alias"), "inner_col2": dy.Int16()})},
            {
                "real_alias": dy.String(),
                "col1.inner_col2": dy.Int16(),
            },  # this is an example of the unexpected behehavior when using both aliases and names
        ),
        (
            {
                "col1": dy.Struct(
                    {
                        "another_struct": dy.Struct(
                            {
                                "another_struct": dy.Struct(
                                    {"yet_another_struct": dy.Struct({"str": dy.String()}, alias="different_alias")}
                                )
                            }
                        ),
                        "col2": dy.Binary(alias="binary_col"),
                    }
                )
            },
            {
                "col1.another_struct.another_struct.different_alias.str": dy.String(),
                "binary_col": dy.Binary(),
            },  # this is an example of the unexpected behehavior when using both aliases and names
        ),
        (
            {"col1": dy.List(dy.Struct({"field1": dy.List(dy.String(), alias="list"), "field2": dy.Bool()}))},
            {"col1.list": dy.String(), "col1.field2": dy.Bool()},
        ),
        (
            {"col1": dy.Array(dy.Int16(), shape=(3, 2))},
            {"col1": dy.Array(dy.Int16(), shape=(3, 2))},
        ),
        ({"col1": dy.Struct({"col2": dy.List(dy.String(alias="col1"))})}, {"col1.col2.col1": dy.String()}),
        (
            {"col1": dy.Struct({"non-nullable": dy.String()}, nullable=True)},
            {"col1.non-nullable": dy.String(nullable=True)},
        ),
    ],
    ids=[
        "no-nesting",
        "simple-list",
        "simple-list-with-alias",
        "nested-list",
        "simple-struct",
        "multi-level-struct",
        "mixed-list-struct",
        "array",
        "repeated-names",
        "nullable-struct",
    ],
)
def test_unnest_columns(columns: dict[str, dy.Column], expected_output: dict[str, dy.Column]) -> None:
    """It preserves all inner columns and names without any nested structures."""
    # ensure that it can *also* accommodate unnested columns
    columns.update(col2=dy.Int16())
    expected_output.update(col2=dy.Int16())

    new_columns = unnest_columns(columns)

    ExpectedOutput: Type[dy.Schema] = type("ExpectedOutput", (dy.Schema,), expected_output)
    NewColumns: Type[dy.Schema] = type("NewColumns", (dy.Schema,), new_columns)

    assert not {  # no nested structures
        k: v for k, v in NewColumns.columns().items() if isinstance(v, (dy.Struct, dy.List))
    }
    assert ExpectedOutput.matches(NewColumns)


@pytest.mark.parametrize(["or_none"], [(True,), (False,)], ids=["nullable", "non-nullable"])
@pytest.mark.parametrize(
    ["input_type", "expected_column", "raises"],
    [
        # Basic types
        (int, dy.Int64(), nullcontext()),
        (str, dy.String(), nullcontext()),
        (float, dy.Float64(), nullcontext()),
        (bool, dy.Bool(), nullcontext()),
        (bytes, dy.Binary(), nullcontext()),
        # Integer constraints - unsigned
        (
            Annotated[int, msgspec.Meta(ge=-1_000_000, le=1_000_000)],
            dy.Int64(min=-1_000_000, max=1_000_000),
            nullcontext(),
        ),
        # Integer constraints - open bounds (gt/lt)
        (
            Annotated[int, msgspec.Meta(gt=-1_000_000, lt=1_000_000)],
            dy.Int64(min_exclusive=-1_000_000, max_exclusive=1_000_000),
            nullcontext(),
        ),
        # Nullable types
        (int, dy.Int64(nullable=True), nullcontext()),
        (str, dy.String(nullable=True), nullcontext()),
        (bool, dy.Bool(nullable=True), nullcontext()),
        (Annotated[int, msgspec.Meta(ge=0, le=255)], dy.Int64(nullable=True, min=0, max=255), nullcontext()),
        # String constraints
        (Annotated[str, msgspec.Meta(min_length=1)], dy.String(min_length=1), nullcontext()),
        (Annotated[str, msgspec.Meta(max_length=100)], dy.String(max_length=100), nullcontext()),
        (
            Annotated[str, msgspec.Meta(min_length=1, max_length=100)],
            dy.String(min_length=1, max_length=100),
            nullcontext(),
        ),
        (Annotated[str, msgspec.Meta(pattern=r"^\d+$")], dy.String(regex=r"^\d+$"), nullcontext()),
        # List types
        (list[str], dy.List(inner=dy.String()), nullcontext()),
        (list[int], dy.List(inner=dy.Int64()), nullcontext()),
        (list[bool], dy.List(inner=dy.Bool()), nullcontext()),
        (list[float], dy.List(inner=dy.Float64()), nullcontext()),
        # Enum types
        (IntEnum("DirectionId", {"OUTBOUND": 0, "INBOUND": 1}), dy.Int64(is_in=[0, 1]), nullcontext()),
        (
            Enum("Color", {"RED": "red", "GREEN": "green", "BLUE": "blue"}),
            dy.Enum(["red", "green", "blue"]),
            nullcontext(),
        ),
        (
            StrEnum("Status", {"ACTIVE": "active", "INACTIVE": "inactive"}),
            dy.Enum(["active", "inactive"]),
            nullcontext(),
        ),
        (Flag("Permissions", {"READ": 1, "WRITE": 2, "EXECUTE": 4}), dy.Int64(is_in=[1, 2, 4]), nullcontext()),
        # Struct types
        (msgspec.defstruct("User", [("field", int)]), dy.Struct(inner={"field": dy.Int64()}), nullcontext()),
        (
            msgspec.defstruct("ComplexUser", [("name", str), ("age", int), ("active", bool)]),
            dy.Struct(inner={"name": dy.String(), "age": dy.Int64(), "active": dy.Bool()}),
            nullcontext(),
        ),
        (
            msgspec.defstruct("NestedStruct", [("inner", msgspec.defstruct("Inner", [("value", str)]))]),
            dy.Struct(inner={"inner": dy.Struct(inner={"value": dy.String()})}),
            nullcontext(),
        ),
        # Nullable struct
        (
            msgspec.defstruct("NullableUser", [("field", int)]),
            dy.Struct(inner={"field": dy.Int64()}, nullable=True),
            nullcontext(),
        ),
        # Lists of complex types
        (list[list[str]], dy.List(inner=dy.List(inner=dy.String())), nullcontext()),
        (list[list[int]], dy.List(inner=dy.List(inner=dy.Int64())), nullcontext()),
        (list[str | None], dy.List(inner=dy.String(nullable=True)), nullcontext()),
        (list[int | None], dy.List(inner=dy.Int64(nullable=True)), nullcontext()),
        (
            list[msgspec.defstruct("User", [("id", int), ("name", str)])],
            dy.List(inner=dy.Struct(inner={"id": dy.Int64(), "name": dy.String()})),
            nullcontext(),
        ),
        (
            list[IntEnum("Status", {"ACTIVE": 1, "INACTIVE": 0})],
            dy.List(inner=dy.Int64(is_in=[1, 0])),
            nullcontext(),
        ),
        (
            list[Enum("Color", {"RED": "red", "BLUE": "blue"})],
            dy.List(inner=dy.Enum(["red", "blue"])),
            nullcontext(),
        ),
        # Fallback for unknown types
        (msgspec.msgpack.Ext, dy.Any(), pytest.raises(NotImplementedError)),
    ],
    ids=[
        "Int64",
        "String",
        "Float64",
        "Bool",
        "Binary",
        "Int64-ge-le",
        "Int64-gt-lt",
        "nullable-Int64",
        "nullable-String",
        "nullable-Bool",
        "nullable-Int64",
        "String-min-length",
        "String-max-length",
        "String-min-max-length",
        "String-pattern",
        "List-String",
        "List-Int64",
        "List-Bool",
        "List-Float64",
        "IntEnum-List",
        "Enum-List",
        "StrEnum-List",
        "Flag-List",
        "Struct-simple",
        "Struct-multiple-fields",
        "Struct-nested",
        "Struct-nullable",
        "List-nested-List-String",
        "List-nested-List-Int",
        "List-String (nullable)",
        "List-Int (nullable)",
        "List-Struct",
        "List-IntEnum",
        "List-Enum",
        "Ext-NotImplemented",
    ],
)
def test_msgspec_to_dataframely_field(
    input_type: msgspec.inspect.Any, or_none: bool, expected_column: dy.Column, raises: pytest.RaisesExc
) -> None:
    """It converts a msgpec field to the appropriate dataframely column or returns a NotImplementedError."""
    if or_none:
        input_type = input_type | None

    class TestStruct(msgspec.Struct):
        field: input_type

    msgspec_field = msgspec.inspect.type_info(msgspec.structs.fields(TestStruct)[0].type)

    with raises:
        dataframely_field = msgspec_to_dataframely_field(msgspec_field)
        assert with_nullable(expected_column, or_none).matches(dataframely_field, pl.col("foo"))


@pytest.mark.parametrize(
    "fields",
    [[("id", int), ("name", str), ("age", int | None)]],
    ids=["simple-struct"],
)
def test_struct_to_schema(fields: list[tuple[str, type]]) -> None:
    """It converts a msgspec struct to a dataframely schema."""
    User: Type[msgspec.Struct] = msgspec.defstruct("User", fields)

    class UserFactory(MsgspecFactory[User]): ...

    user_instance = UserFactory.build()
    assert isinstance(user_instance, User)
    dy_schema = struct_to_schema(User)

    pl.read_json(msgspec.json.encode(user_instance), schema=dy_schema.to_polars_schema())
