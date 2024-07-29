from typing import List

from lamp_py.mssql.mssql_utils import MSSQLManager
from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.ingestion_tm.jobs.whole_table import (
    TMMainGeoNode,
    TMMainRoute,
    TMMainTrip,
    TMMainVehicle,
    TMMainsScheduledWaiver,
)
from lamp_py.ingestion_tm.jobs.parition_table import TMDailyLogStopCrossing


def ingest_tables() -> None:
    """
    ingest tables from transmaster database
    """
    tm_db = MSSQLManager(verbose=True)

    jobs: List[TMExport] = [
        TMMainGeoNode(),
        TMMainRoute(),
        TMMainTrip(),
        TMMainVehicle(),
        TMMainsScheduledWaiver(),
        TMDailyLogStopCrossing(),
    ]

    for job in jobs:
        job.run_export(tm_db)
