from datetime import datetime

import dataframely as dy
import pytest
import polars as pl

from lamp_py.ingestion.glides import GlidesConverter, EditorChanges, OperatorSignIns, TripUpdates, VehicleTripAssignment


@pytest.mark.parametrize(
    [
        "converter",
    ],
    [
        (EditorChanges(),),
        (OperatorSignIns(),),
        (TripUpdates(),),
        (VehicleTripAssignment(),),
    ],
    ids=["editor-changes", "operator-sign-ins", "trip-updates", "vehicle-trip-assignments"],
)
def test_convert_records(dy_gen: dy.random.Generator, converter: GlidesConverter, num_rows: int = 5) -> None:
    """It returns datasets with the expected schema."""
    converter.records = converter.Record.sample(
        num_rows=num_rows,
        generator=dy_gen,
        overrides={
            "time": dy_gen.sample_datetime(
                num_rows, min=datetime(2024, 1, 1), max=datetime(2037, 12, 31)
            )  # within serlializable timestamp range
        },
    ).to_dicts()

    table = pl.scan_pyarrow_dataset(converter.convert_records())

    assert not converter.Table.validate(table).is_empty()
    assert set(converter.Table.column_names()) == set(table.collect_schema().names())  # no extra columns
