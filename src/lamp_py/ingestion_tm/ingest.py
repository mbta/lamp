from typing import List

from lamp_py.mssql.mssql_utils import MSSQLManager
from lamp_py.ingestion_tm.tm_export import TMExport
from lamp_py.ingestion_tm.jobs.whole_table import (
    TMMainGeoNode,
    TMMainRoute,
    TMMainTrip,
    TMMainVehicle,
    TMMainBlock,
    TMMainOperator,
    TMMainRun,
    TMMainWorkPiece,
    TMMainPatternGeoNodeXref,
    TMMainTimePoint,
    TMDailyLogDailySchedAdhereWaiver,
)
from lamp_py.ingestion_tm.jobs.parition_table import (
    TMDailyLogStopCrossing,
    TMDailyLogDailyWorkPiece,
    TMDailyLogLoggedMessage,
)


def get_ingestion_jobs() -> List[TMExport]:
    """
    get a list of all ingestion jobs that
    """
    return [
        TMMainGeoNode(),
        TMMainRoute(),
        TMMainTrip(),
        TMMainVehicle(),
        TMMainBlock(),
        TMMainOperator(),
        TMMainRun(),
        TMMainWorkPiece(),
        TMMainPatternGeoNodeXref(),
        TMMainTimePoint(),
        TMDailyLogStopCrossing(),
        TMDailyLogDailyWorkPiece(),
        TMDailyLogDailySchedAdhereWaiver(),
        TMDailyLogLoggedMessage(),
    ]


def ingest_tables() -> None:
    """
    ingest tables from transmaster database
    """
    tm_db = MSSQLManager(verbose=True)
    jobs: List[TMExport] = get_ingestion_jobs()

    for job in jobs:
        job.run_export(tm_db)
