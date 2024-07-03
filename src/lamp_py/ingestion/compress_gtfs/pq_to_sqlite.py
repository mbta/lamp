import os
import sqlite3

import pyarrow
import pyarrow.dataset as pd

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.ingestion.utils import gzip_file


def sqlite_type(pq_type: str) -> str:
    """
    return SQLITE type from pyarrow Field type
    """
    if "int" in pq_type:
        return "INTEGER"
    if "bool" in pq_type:
        return "INTEGER"
    if "float" in pq_type:
        return "REAL"
    if "double" in pq_type:
        return "REAL"
    return "TEXT"


def sqlite_table_query(table_name: str, schema: pyarrow.Schema) -> str:
    """
    return CREATE TABLE query for sqlite table from pyarrow schema
    """
    logger = ProcessLogger("sqlite_create_table")
    logger.log_start()
    field_list = [
        f"{field.name} {sqlite_type(str(field.type))}" for field in schema
    ]
    query = f"""
        CREATE TABLE 
        IF NOT EXISTS 
        {table_name}
        (
            {','.join(field_list)}
        );
    """
    logger.log_complete()
    return query


def pq_folder_to_sqlite(year_path: str) -> None:
    """
    load all files from year_path folder into SQLITE3 db file
    """
    logger = ProcessLogger("pq_to_sqlite", year_path=year_path)
    logger.log_start()

    db_path = os.path.join(year_path, "GTFS_ARCHIVE.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    try:
        for file in os.listdir(year_path):
            if ".parquet" not in file:
                continue
            logger.add_metadata(current_file=file)

            ds = pd.dataset(os.path.join(year_path, file))

            table = file.replace(".parquet", "")
            columns = [f":{col}" for col in ds.schema.names]
            insert_query = f"INSERT INTO {table} VALUES({','.join(columns)});"

            conn = sqlite3.connect(db_path)
            with conn:
                conn.execute(sqlite_table_query(table, ds.schema))
            with conn:
                for batch in ds.to_batches(batch_size=250_000):
                    conn.executemany(insert_query, batch.to_pylist())
            conn.close()

        gzip_file(db_path)

        logger.log_complete()
    except Exception as exception:
        logger.log_failure(exception)
