from typing import List
import pytest

from lamp_py.tableau.conversions.converter import Converter
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns, HyperGlidesTripUpdates


def start_glides_parquet_updates() -> None:
    """Run all Glides Parquet Update jobs"""

    parquet_update_jobs: List[Converter] = [
        HyperGlidesTripUpdates(),
        HyperGlidesOperatorSignIns(),
    ]

    for job in parquet_update_jobs:
        job.run_parquet(None)


@pytest.mark.tableau
def test_glides_parquet_updates():
    start_glides_parquet_updates()
