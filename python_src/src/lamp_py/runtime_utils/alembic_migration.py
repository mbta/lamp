import subprocess
import logging

from lamp_py.postgres.postgres_utils import DatabaseManager

def run_alembic_migration(db_name) -> None:
    """
    run alembic migration command at application startup
    """

    if db_name == "performance_manager":
        # if database is clean, call to DatabaseManager will trigger 
        # SqlBase.metadata.create_all(self.engine)
        # creating all database tables
        db_manager = DatabaseManager()
    else:
        raise Exception(f"Migration for {db_name} not implemented.")
    
    # check if alembic_version table exists in rds
    try:
        db_manager.select_as_list("SELECT 1 FROM alembic_version")
        version_table_exists = True
    except Exception as _:
        version_table_exists = False

    if version_table_exists:
        # run normal migration if version table exists
        migration_command = [
            "alembic",
            "-n",
            db_name,
            "upgrade",
            "head",
        ]
    else:
        # if no version table, create version table and stamp with current head
        # this assumes the head matches current rds state
        migration_command = [
            "alembic",
            "-n",
            db_name,
            "stamp",
            "head",
        ]

    result = subprocess.run(migration_command, capture_output=True, text=True)
    logging.info(result.stdout)
    logging.info(result.stderr)
    