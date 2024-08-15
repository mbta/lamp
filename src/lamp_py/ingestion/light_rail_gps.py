import os
from typing import (
    List,
    Optional,
    Tuple,
)
from threading import current_thread
from concurrent.futures import ThreadPoolExecutor

from pyarrow import fs
import polars as pl

from lamp_py.aws.s3 import (
    move_s3_objects,
    file_list_from_s3,
    object_exists,
)
from lamp_py.runtime_utils.process_logger import ProcessLogger

from lamp_py.ingestion.utils import DEFAULT_S3_PREFIX

raw_gps_schema = pl.Schema(
    {
        "serial_number": pl.String(),
        "speed": pl.Float64(),
        "date": pl.Date(),
        "updated_at": pl.String(),
        "bearing": pl.Float64(),
        "latitude": pl.String(),
        "longitude": pl.String(),
        "car": pl.String(),
    }
)


def thread_init(file: str) -> None:
    """
    initialize the filesystem in each read thread

    :param file: example file to determine file system type
    """
    thread_data = current_thread()
    if file.startswith("s3://"):
        thread_data.__dict__["file_system"] = fs.S3FileSystem()
    else:
        thread_data.__dict__["file_system"] = fs.LocalFileSystem()


def thread_gps_to_frame(path: str) -> Tuple[Optional[pl.DataFrame], str]:
    """
    gzip to dataframe converter function meant to be run in ThreadPool

    :param path: path to gzip that will be converted to polars dataframe
    """
    file_system = current_thread().__dict__["file_system"]
    path = path.replace("s3://", "")

    logger = ProcessLogger(process_name="light_rail_gps_to_frame", path=path)
    logger.log_start()

    try:
        with file_system.open_input_stream(path, compression="gzip") as f:
            df = (
                pl.read_json(f.read())
                .transpose(
                    include_header=True,
                    header_name="serial_number",
                    column_names=("data",),
                )
                .select(
                    pl.col("serial_number").cast(pl.String),
                    pl.col("data").struct.field("speed").cast(pl.Float64),
                    (
                        pl.col("data")
                        .struct.field("updated_at")
                        .str.slice(0, length=10)
                        .str.to_date()
                        .alias("date")
                    ),
                    pl.col("data").struct.field("updated_at").cast(pl.String),
                    pl.col("data").struct.field("bearing").cast(pl.Float64),
                    pl.col("data").struct.field("latitude").cast(pl.String),
                    pl.col("data").struct.field("longitude").cast(pl.String),
                    pl.col("data").struct.field("car").cast(pl.String),
                )
            )

        logger.log_complete()

    except Exception as exception:
        logger.log_failure(exception)
        df = None

    return (df, path)


def dataframe_from_gz(
    files: List[str],
) -> Tuple[pl.DataFrame, List[str], List[str]]:
    """
    create polars dataframe from list of file paths using ThreadPool

    :param files: list of gzip file paths to convert to dataframe

    :return
    dataframe:
        serial_number: String,
        speed: Float64,
        date: Date,
        updated_at: String,
        bearing: Float64,
        latitude: String,
        longitude: String,
        car: String,

    archive_files: correctly loaded gzip paths
    error_files: gzip paths that failed to load
    """
    logger = ProcessLogger(
        process_name="light_rail_df_from_gz", num_files=len(files)
    )
    logger.log_start()

    init_file = files[0]
    dfs = []
    archive_files = []
    error_files = []

    try:
        with ThreadPoolExecutor(
            max_workers=16, initializer=thread_init, initargs=(init_file,)
        ) as pool:
            for df, path in pool.map(thread_gps_to_frame, files):
                if df is not None:
                    archive_files.append(path)
                    dfs.append(df)
                else:
                    error_files.append(path)

        dataframe: pl.DataFrame = pl.concat(dfs)

        logger.add_metadata(
            num_archive_files=len(archive_files),
            num_error_files=len(error_files),
        )
        logger.log_complete()

    except Exception as exception:
        error_files += files
        archive_files = []
        dataframe = pl.DataFrame(schema=raw_gps_schema)
        logger.log_failure(exception)

    return (dataframe, archive_files, error_files)


def write_parquet(dataframe: pl.DataFrame) -> None:
    """
    sync and write parquet files partitioned by date column

    will merge any existing parquet files in memory
    """
    for date in dataframe.get_column("date").unique():
        logger = ProcessLogger(
            process_name="light_rail_write_parquet", date=date
        )
        logger.log_start()

        remote_obj = os.path.join(
            os.environ["PUBLIC_ARCHIVE_BUCKET"],
            DEFAULT_S3_PREFIX,
            "light_rail_gps",
            f"year={date.year}",
            f"{date.isoformat()}.parquet",
        )
        day_frame = dataframe.filter(pl.col("date") == date)

        if object_exists(remote_obj):
            day_frame = pl.concat(
                [day_frame, pl.read_parquet(f"s3://{remote_obj}")]
            )

        day_frame = day_frame.unique().sort(by=["serial_number", "updated_at"])

        logger.add_metadata(parquet_rows=day_frame.shape[0])

        day_frame.write_parquet(f"s3://{remote_obj}", use_pyarrow=True)

        logger.log_complete()


def ingest_light_rail_gps() -> None:
    """
    retrieve group of LightRailRawGPS gz files from INCOMING bucket

    convert files into single dataframe

    merge dataframe with any existing export parquet files and write new parquet
    files partitioned by date from "updated_at" field
    """
    logger = ProcessLogger(process_name="ingest_light_rail_gps")
    logger.log_start()

    try:
        s3_files = file_list_from_s3(
            bucket_name=os.environ["INCOMING_BUCKET"],
            file_prefix=DEFAULT_S3_PREFIX,
            max_list_size=5_000,
        )

        s3_files = [file for file in s3_files if "LightRailRawGPS" in file]

        dataframe, archive_files, error_files = dataframe_from_gz(s3_files)

        write_parquet(dataframe)

        logger.log_complete()

    except Exception as exception:
        logger.log_failure(exception)

    if len(archive_files) > 0:
        move_s3_objects(archive_files, os.environ["ARCHIVE_BUCKET"])
    if len(error_files) > 0:
        move_s3_objects(error_files, os.environ["ERROR_BUCKET"])
