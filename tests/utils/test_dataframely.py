import dataframely as dy
import pytest

from lamp_py.utils.dataframely import with_alias, unnest_columns


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
    ],
)
def test_unnest_columns(columns: dict[str, dy.Column], expected_output: dict[str, dy.Column]) -> None:
    """It preserves all inner columns and names without any nested structures."""
    # ensure that it can *also* accommodate unnested columns
    columns.update(col2=dy.Int16())
    expected_output.update(col2=dy.Int16())

    new_columns = unnest_columns(columns)

    ExpectedOutput = type("ExpectedOutput", (dy.Schema,), expected_output)
    NewColumns = type("NewColumns", (dy.Schema,), new_columns)

    assert not {  # no nested structures
        k: v for k, v in NewColumns.columns().items() if isinstance(v, (dy.Struct, dy.List))  # type: ignore[attr-defined]
    }
    assert ExpectedOutput.matches(NewColumns)  # type: ignore[attr-defined]
