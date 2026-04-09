from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import dataframely as dy
import polars as pl
import pytest
from pytest_mock import MockerFixture

from lamp_py.ingestion.glides import GlidesRecord, OperatorSignInsTable, TripUpdatesTable
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns, HyperGlidesTripUpdates


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
    mocker: MockerFixture,
    dy_gen: dy.random.Generator,
    tmp_path: Path,
    num_records: int = 10,
) -> None:
    """It creates a compatible Parquet file."""
    springboard_path = tmp_path / "springboard.parquet"
    (
        schema.sample(
            num_records,
            generator=dy_gen,
            overrides={
                "time": dy_gen.sample_datetime(
                    num_records, min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="us"
                ).cast(pl.Datetime(time_unit="ms"))
            },
        ).write_parquet(springboard_path.as_posix())
    )

    mocker.patch("lamp_py.tableau.jobs.glides.file_list_from_s3", return_value=[springboard_path.as_uri()])

    test_path = tmp_path.joinpath("test.parquet")
    mocker.patch.object(job, "local_parquet_path", test_path.as_posix())

    with raises:
        job.create_parquet(None)

        assert pl.read_parquet(test_path).height == num_records
