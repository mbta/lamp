import os
import duckdb

from lamp_py.runtime_utils.process_logger import ProcessLogger

DEFAULT_BUCKET_URI = "s3://mbta-ctd-dataplatform-springboard/lamp/"
TIMESTAMP_PARTITIONED_DATASETS = {
    "SHAPES",
    "CALENDAR",
}

def create_read_date_partitioned(uri: str = DEFAULT_BUCKET_URI) -> None:
    "Write a function in database that reads from parquet the provided dates and dataset."
    duckdb.sql(f"""
    CREATE OR REPLACE MACRO read_date_partitioned

        (dataset, start_date, end_date) AS TABLE (
        SELECT * FROM 
            read_parquet(
                list_transform(
                    range(
                        start_date,
                        end_date,
                        INTERVAL 1 DAY
                    ),
                    lambda x : strftime(
                        x,
                        '{uri}'
                        || upper(dataset)
                        || '/year=%Y/month=%-m/day=%-d/%xT%H:%M:%S.parquet'
                    )
                )
            )
        ),

        (dataset, date_list) AS TABLE (
        SELECT * FROM
            read_parquet(
                list_transform(
                    date_list,
                    lambda x : strftime(
                        x,
                        '{uri}'
                        || upper(dataset)
                        || '/year=%Y/month=%-m/day=%-d/%xT%H:%M:%S.parquet'
                    )
                )
            )
        )
    """)

def create_timestamp_partitioned_views(datasets: set[str] = TIMESTAMP_PARTITIONED_DATASETS) -> set[str]:
    "Create views in database by globbing directory structure for shallowly nested files."
    process_logger = ProcessLogger("create_timestamp_partitioned_views")

    for dataset in datasets:
        (
            duckdb
            .from_parquet(DEFAULT_BUCKET_URI + dataset + "/*/*.parquet", hive_partitioning = True)
            .create_view(dataset.lower())
        )
        process_logger.add_metadata(view_created = dataset)
    
    process_logger.log_complete()

    return datasets

def save_db_locally(file_path: str = "/tmp/lamp.db") -> str:
    "Copy the in-memory database to a local path."
    try:
        duckdb.sql("DETACH local")
    except duckdb.BinderException:
        pass
    
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass

    duckdb.sql(f"ATTACH '{file_path}' AS local")
    duckdb.sql("COPY FROM DATABASE memory TO local")
    duckdb.sql("DETACH local")

    return file_path
