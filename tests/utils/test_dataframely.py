from contextlib import nullcontext
from typing import Type

import dataframely as dy
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from lamp_py.utils.dataframely import (
    drop_pii_columns,
    extract_pii_columns,
    has_metadata,
    unnest_columns,
    with_alias,
    with_nullable,
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
        (dy.UInt16,),
        (dy.UInt32,),
        (dy.UInt64,),
        (dy.UInt8,),
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
                "col1.real_alias": dy.String(),
                "col1.inner_col2": dy.Int16(),
            },
        ),
        (
            {
                "data": dy.Struct(
                    {
                        "metadata": dy.Struct(
                            {
                                "author": dy.Struct({"emailAddress": dy.String()}),
                            },
                        ),
                        "changes": dy.List(
                            dy.Struct(
                                {
                                    "editor": dy.Struct({"emailAddress": dy.String()}),
                                }
                            )
                        ),
                        "col2": dy.Binary(alias="binary_col"),
                    }
                )
            },
            {
                "data.metadata.author.emailAddress": dy.String(),
                "data.changes.editor.emailAddress": dy.String(),
                "data.binary_col": dy.Binary(),
            },
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

    _ = unnest_columns(columns)  # test side effects on columns by performing operation twice
    new_columns = unnest_columns(columns)

    ExpectedOutput: Type[dy.Schema] = type("ExpectedOutput", (dy.Schema,), expected_output)
    NewColumns: Type[dy.Schema] = type("NewColumns", (dy.Schema,), new_columns)

    assert not {  # no nested structures
        k: v for k, v in NewColumns.columns().items() if isinstance(v, (dy.Struct, dy.List))
    }
    assert ExpectedOutput.matches(NewColumns)


@pytest.mark.parametrize(
    ["column", "key", "expected_result"],
    [
        (dy.String(metadata={"key1": "value1", "key2": "value2"}), "key1", True),
        (dy.String(metadata={"key1": "value1", "key2": "value2"}), "key3", False),
        (dy.Int16(), "any_key", False),
    ],
    ids=[
        "dataframely-metadata-key-present",
        "dataframely-metadata-key-absent",
        "dataframely-no-metadata",
    ],
)
def test_has_metadata(column: dy.Column | pa.Field, key: str, expected_result: bool) -> None:
    """It correctly determines the presence of metadata keys."""
    assert has_metadata(column, key) == expected_result


@pytest.mark.parametrize(
    "schema_type",
    [
        "dataframely",
    ],
)
@pytest.mark.parametrize("has_pii", [False, True], ids=["no-pii", "has-pii"])
def test_extract_pii_columns(schema_type: str, has_pii: bool) -> None:
    """It handles multiple input schema types and the presence or absence of PII."""
    if has_pii:
        metadata = {
            "pii_roles": [
                "foo",
            ]
        }
    else:
        metadata = {}

    if schema_type == "dataframely":
        schema: Type[dy.Schema] = type("TestSchema", (dy.Schema,), {"col1": dy.String(metadata=metadata)})

    cols = extract_pii_columns(schema)

    assert bool(cols) == has_pii


@pytest.mark.parametrize(
    [
        "columns",
        "raises",
    ],
    [
        ({"col1": dy.String()}, nullcontext()),
        (
            {"col1": dy.String(primary_key=True, metadata={"pii_roles": ["foo"]})},
            pytest.raises(AssertionError, match="Splitting would remove"),
        ),
        ({"col1": dy.String(primary_key=True), "col2": dy.Int8(metadata={"pii_roles": ["foo"]})}, nullcontext()),
        (
            {
                "col1": dy.String(primary_key=True),
                "col2": dy.String(primary_key=True),
                "col3": dy.Int8(metadata={"pii_roles": ["foo"]}),
                "col4": dy.Int8(metadata={"pii_roles": ["bar"]}),
            },
            nullcontext(),
        ),
    ],
    ids=["no-pii", "pii-in-keys", "1-pii-col", "multiple-pii-cols"],
)
def test_drop_pii_columns(columns: dict[str, dy.Column], raises: pytest.RaisesExc, dy_gen: dy.random.Generator) -> None:
    """It leaves no columns marked as PII."""
    schema: Type[dy.Schema] = type("TestSchema", (dy.Schema,), columns)

    df = schema.sample(generator=dy_gen)

    with raises:
        non_sensitive_df = drop_pii_columns(df, schema)

        assert set(df.columns).difference(set(non_sensitive_df.columns)) == set(
            col.name for col in extract_pii_columns(schema)
        )
        assert_frame_equal(df.drop(col.name for col in extract_pii_columns(schema)), non_sensitive_df)
