import os
from typing import List

from lamp_py.aws.ecs import check_for_parallel_tasks
from lamp_py.runtime_utils.env_validation import validate_environment
from lamp_py.tableau.hyper import HyperJob
from lamp_py.tableau.jobs.bus_performance import HyperBusPerformanceAll, HyperBusPerformanceRecent
from lamp_py.tableau.pipeline import start_bus_parquet_updates
from lamp_py.tableau.server import datasource_from_name


def start_bus_hyper() -> None:
    """Run all HyperFile Update Jobs"""
    # configure the environment
    os.environ["SERVICE_NAME"] = "tableau_hyper_update"

    # os.environ["TABLEAU_PROJECT"] = "GTFS-RT"

    validate_environment(
        required_variables=[
            "TABLEAU_USER",
            "TABLEAU_PASSWORD",
            "TABLEAU_SERVER",
            "TABLEAU_PROJECT",
            "PUBLIC_ARCHIVE_BUCKET",
            # "ECS_CLUSTER",
            # "ECS_TASK_GROUP",
        ],
        private_variables=[
            "TABLEAU_PASSWORD",
        ],
    )

    # make sure only one publisher runs at a time
    check_for_parallel_tasks()

    hyper_jobs: List[HyperJob] = [
        # HyperBusPerformanceAll(),
        HyperBusPerformanceRecent(),
    ]

    for job in hyper_jobs:
        outs = job.run_hyper()


if __name__ == "__main__":

    start_bus_hyper()
