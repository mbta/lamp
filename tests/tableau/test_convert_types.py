import polars as pl
import polars.testing as pt
from io import StringIO

from lamp_py.tableau.spare.default_converter import PolarsDataFrameConverter


def test_convert_to_tableau_flat_schema() -> None:
    """
    Test convert_to_tableau_flat expansion of lists of structs
    """
    df1 = pl.DataFrame(
        {
            "data": [[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], [{"name": "Charlie", "age": 40}], []],
            "data2": [
                [{"name": "Darcy", "age": 30}, {"name": "Effie", "age": 25}],
                [{"name": "Gerard", "age": 40}, {"name": "Henry", "age": 30}, {"name": "Iggy", "age": 25}],
                [{"name": "Juliet", "age": 40}],
            ],
        }
    )
    print(df1)

    expected = pl.read_json("tests/tableau/test_convert_types_expected.json")

    pt.assert_frame_equal(expected, PolarsDataFrameConverter.convert_to_tableau_flat_schema(df1))
