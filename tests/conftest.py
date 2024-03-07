"""
this file contains fixtures that are intended to be used across multiple test
files
"""

from typing import (
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pyarrow import fs, parquet, Table


@pytest.fixture(autouse=True, name="get_pyarrow_table_patch")
def fixture_get_pyarrow_table_patch(monkeypatch: MonkeyPatch) -> Iterator[None]:
    """
    the aws.s3 function `_get_pyarrow_table` function reads parquet files from
    s3 and returns a pyarrow table. when testing on our github machines, we
    don't have access to s3, so all tests must be run against local files.
    monkeypatch the function to read from a local filepath.
    """

    def mock__get_pyarrow_table(
        filename: Union[str, List[str]],
        filters: Optional[Union[Sequence[Tuple], Sequence[List[Tuple]]]] = None,
    ) -> Table:
        active_fs = fs.LocalFileSystem()

        if isinstance(filename, list):
            to_load = filename
        else:
            to_load = [filename]

        if len(to_load) == 0:
            return Table.from_pydict({})

        return parquet.ParquetDataset(
            to_load, filesystem=active_fs, filters=filters
        ).read_pandas()

    monkeypatch.setattr(
        "lamp_py.aws.s3._get_pyarrow_table", mock__get_pyarrow_table
    )

    yield
