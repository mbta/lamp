import polars as pl
import polars.testing as pt
from io import StringIO

from lamp_py.tableau.spare.default_converter import convert_to_tableau_flat_schema


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

    json_str = '[{"data.name":"Alice","data.age":30,"data2.name":"Darcy","data2.age":30},{"data.name":"Alice","data.age":30,"data2.name":"Effie","data2.age":25},{"data.name":"Bob","data.age":25,"data2.name":"Darcy","data2.age":30},{"data.name":"Bob","data.age":25,"data2.name":"Effie","data2.age":25},{"data.name":"Charlie","data.age":40,"data2.name":"Gerard","data2.age":40},{"data.name":"Charlie","data.age":40,"data2.name":"Henry","data2.age":30},{"data.name":"Charlie","data.age":40,"data2.name":"Iggy","data2.age":25},{"data.name":null,"data.age":null,"data2.name":"Juliet","data2.age":40}]'
    expected = pl.read_json(StringIO(json_str))

    pt.assert_frame_equal(expected, convert_to_tableau_flat_schema(df1))
