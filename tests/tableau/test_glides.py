from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import dataframely as dy
import polars as pl
import pytest
from pyarrow.dataset import Dataset
from pytest_mock import MockerFixture

from lamp_py.ingestion.glides import GlidesRecord, OperatorSignInsTable, TripUpdatesTable
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns, HyperGlidesTripUpdates
from lamp_py.utils.dataframely import has_metadata


@pytest.mark.parametrize(
    ["job", "schema", "raises"],
    [
        (HyperGlidesOperatorSignIns(), OperatorSignInsTable, nullcontext(False)),
        (HyperGlidesTripUpdates(), TripUpdatesTable, nullcontext(False)),
        (HyperGlidesTripUpdates(), OperatorSignInsTable, pytest.raises(pl.exceptions.ColumnNotFoundError)),
    ],
    ids=["operator_sign_ins", "trip_updates", "mismatched_schema"],
)
def test_glides_hyper_job(
    job: HyperJob,
    schema: GlidesRecord,
    raises: pytest.RaisesExc,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
    dy_gen: dy.random.Generator,
    tmp_path: Path,
    num_records: int = 10,
) -> None:
    """It creates a compatible Parquet file."""
    sample_data = (
        schema.sample(
            num_records,
            generator=dy_gen,
            overrides={
                "time": dy_gen.sample_datetime(
                    num_records, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
                ).cast(pl.Datetime(time_unit="ms"))
            },
        )
        .to_arrow()
        .to_batches()
    )

    monkeypatch.setattr("lamp_py.aws.s3.file_list_from_s3", ["foo", "bar"])
    mock_dataset = mocker.MagicMock(Dataset)
    mock_dataset.to_batches.return_value = sample_data
    mocker.patch("pyarrow.dataset.dataset", return_value=mock_dataset)

    test_path = tmp_path.joinpath("test.parquet")
    monkeypatch.setattr(job, "local_parquet_path", test_path)

    pii_columns = [k for k, v in schema.columns().items() if has_metadata(v, "reader_roles")]

    with raises:
        job.create_parquet(None)

        assert pl.read_parquet(test_path).height == num_records
        assert all(col not in pl.read_parquet_schema(test_path).keys() for col in pii_columns)
