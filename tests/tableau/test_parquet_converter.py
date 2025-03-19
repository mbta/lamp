from typing import List
import pytest

from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns, HyperGlidesTripUpdates


def start_glides_parquet_updates() -> None:
    """Run all Glides Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),
        HyperGlidesOperatorSignIns(),
    ]

    for job in parquet_update_jobs:
        breakpoint()
        job.run_parquet(None)


# @pytest.mark.tableau
def test_glides_parquet_updates() -> None:
    start_glides_parquet_updates()


if __name__ == "__main__":
    start_glides_parquet_updates()
