from typing import List

from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.glides import HyperGlidesOperatorSignIns, HyperGlidesTripUpdates


# don't run this in pytest - environment variables in pyproject.toml point to local SPRINGBOARD/ARCHIVE
# need the .env values to run
def start_glides_parquet_updates() -> None:
    """Run all Glides Parquet Update jobs"""

    parquet_update_jobs: List[HyperJob] = [
        HyperGlidesTripUpdates(),
        HyperGlidesOperatorSignIns(),
    ]

    for job in parquet_update_jobs:
        breakpoint()
        job.run_parquet(None)
        outs = job.create_local_hyper()
        print(outs)


if __name__ == "__main__":
    start_glides_parquet_updates()
