import inspect
from typing import Set, List

from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.ingestion_tm.ingest import get_ingestion_jobs


def get_tm_export_subclasses(
    cls: type[TMExport] = TMExport,
) -> Set[type[TMExport]]:
    """
    recursively get all of the concrete TMExport child classes
    """
    subclasses: List[type[TMExport]] = []
    for subclass in cls.__subclasses__():
        if inspect.isabstract(subclass):
            subclasses += get_tm_export_subclasses(subclass)
        else:
            subclasses.append(subclass)

    return set(subclasses)


def test_ingestion_job_count() -> None:
    """
    test that the ingestion pipeline is aware of each tm export class
    """
    # get all of the jobs run in ingestion, assert its not empty
    ingestion_jobs = get_ingestion_jobs()
    job_types = {type(job) for job in ingestion_jobs}
    assert job_types

    # get all potential jobs based on subclasses. assert its not empty
    all_job_types = get_tm_export_subclasses()
    assert all_job_types

    # ensure all job types are accounted for in ingestion
    assert (
        all_job_types == job_types
    ), f"Missing instances for subclasses: {all_job_types - job_types}"
