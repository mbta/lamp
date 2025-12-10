from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
import time
from typing import Dict, Iterable, List, Optional
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE
import polars as pl

# read everything from a day in archive
# parse it json like before...rerun processing, spit out tmp file
# ensure the file name is in different partition
# output back to springboard

# is this a good way to do it?
# kind of want...merge sort or something.
# process files is running 1 worker for each type. so this will be slow.
# want to run N workers for all of the files.

# test gz to pyarrow vs gz to polars - time it

# specialize GtfsRtConverter


class GtfsRtAdHocConverter(GtfsRtConverter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    """

    def __init__(
        self,
        config_type: ConfigType,
        metadata_queue: Queue[Optional[str]],
        output_location: str,
        polars_filter: pl.Expr,
        max_workers: int = 4,
    ) -> None:
        GtfsRtConverter.__init__(self, config_type, metadata_queue, max_workers=max_workers)

        self.detail = RtTripDetail()

        self.tmp_folder = output_location

        self.data_parts: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []
        self.filter = polars_filter

    def convert(self) -> None:

        process_logger = ProcessLogger(
            "parquet_table_creator",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        try:
            for table in self.process_files():
                if table.num_rows == 0:
                    continue
                partition_dt = self.partition_dt(table)

                local_path = os.path.join(
                    self.tmp_folder,
                    LAMP,
                    str(self.config_type),
                    f"year={partition_dt.year}",
                    f"month={partition_dt.month}",
                    f"day={partition_dt.day}",
                    f"{partition_dt.isoformat()}_part_{str(table_count)}.parquet",
                )
                os.makedirs(Path(local_path).parent, exist_ok=True)

                self.write_local_pq(table, local_path)

                pool = pyarrow.default_memory_pool()
                pool.release_unused()
                table_count += 1
                process_logger.add_metadata(table_count=table_count)

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.data_parts = {}
            # self.move_s3_files()
            # self.clean_local_folders()

    def write_local_pq(self, table: pyarrow.Table, local_path: str) -> None:
        """
        just write the file out..
        """
        print("running GtfsRtTripUpdatesConverter::write_local_pq")

        writer = pq.ParquetWriter(local_path, schema=table.schema, compression="zstd", compression_level=3)
        writer.write_table(table)
        writer.close()

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        iterate through all of the files to be converted - apply a polars filter to narrow results at the source to reduce write churn - filter at json.gz input level

        only yield a new table when table size crosses over min_rows of yield_check
        """

        process_logger = ProcessLogger(
            "create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(max_workers=self.max_workers, initializer=self.thread_init) as pool:
            for result_dt, result_filename, rt_data in pool.map(self.gz_to_pyarrow, self.files):
                # errors in gtfs_rt conversions are handled in the gz_to_pyarrow
                # function. if one is encountered, the datetime will be none. log
                # the error and move on to the next file.
                if result_dt is not None:
                    logging.info(
                        "processing: %s",
                        result_filename,
                    )
                else:
                    logging.error(
                        "skipping processing: %s",
                        result_filename,
                    )
                    continue

                # create key for self.data_parts dictionary
                dt_part = datetime(
                    year=result_dt.year,
                    month=result_dt.month,
                    day=result_dt.day,
                )
                # create new self.table_groups entry for key if it doesn't exist
                if dt_part not in self.data_parts:
                    self.data_parts[dt_part] = TableData()
                    tmp = self.detail.transform_for_write(rt_data)
                    df = pl.from_arrow(tmp)
                    self.data_parts[dt_part].table = df.filter(self.filter).to_arrow()

                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [
                            self.data_parts[dt_part].table,
                            pl.from_arrow(self.detail.transform_for_write(rt_data)).filter(self.filter).to_arrow(),
                        ]
                    )

                self.data_parts[dt_part].files.append(result_filename)

                yield from self.yield_check(process_logger)

        # yield any remaining tables
        yield from self.yield_check(process_logger, min_rows=-1)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()


# # Define the path to your input Parquet files (can use a glob pattern)
# input_path = "/tmp/test/"
# output_file = "/tmp/combined_file.parquet"
# output_file2 = "/tmp/combined_file_parquet_writer.parquet"

# # Create a dataset from the input files
# ds = pd.dataset(input_path, format="parquet")


# ## experiment 1 - 2gb. exploded. Why?
# # Read the dataset and write it to a single Parquet file
# # pd.write_dataset(ds, output_file)
# # pq.ParquetWriter(hash_pq_path, schema=out_ds.schema,

# # # experiment 2 - no compression. 1.3gb
# # with pq.ParquetWriter(output_file2, schema=ds.schema) as writer:
# #     for batch in ds.to_batches(batch_size=512 * 1024):
# #         writer.write_batch(batch)


# # experiment 3 - compression. 600mb
# with pq.ParquetWriter(output_file2, schema=ds.schema, compression="zstd", compression_level=3) as writer:
#     for batch in ds.to_batches(batch_size=512 * 1024):
#         writer.write_batch(batch)


# breakpoint()
# exit()
# output_prefix = os.path.join(S3_SPRINGBOARD, "DEV_GREEN_UNFILTERED_TRIP_UPDATES")

# read from delta archive between dates
# s3://mbta-ctd-dataplatform-archive/lamp/delta/2025/11/01/2025-11-01T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
bucket_filter = "lamp/delta"
# tm_template = "1{yy}{mm:02}{dd:02}"

# path_template = f"{yy}/{mm:02}/{dd:02}/{yy}-{mm:02}-{dd:02}T00:00:00Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz"
start_date = datetime(2025, 12, 1, 0, 0, 0)
end_date = datetime(2025, 12, 2, 0, 0, 0)
# end_date = datetime(2025, 11, 24, 23, 59, 59)
# start_time = time.time()
logger = ProcessLogger("backfiller")

# timedelta = end_date - start_date
# Generate a list of datetime objects for each hour in a day
# dt.timedelta(hours=x) adds the correct offset to the start time
all_times = [start_date + timedelta(seconds=x) for x in range(24 * 60 * 60)]

# curtime = start_date
# Use a list comprehension to format the datetime objects into file paths
# .strftime('%Y-%m-%d_%H-%M-%S') formats the datetime object into a string
# os.path.join handles path concatenation for different operating systems
file_list = [
    os.path.join("s3://", S3_ARCHIVE, LAMP, "delta", f"{curtime.year}/{curtime.month:02}/{curtime.day:02}/{curtime.year}-{curtime.month:02}-{curtime.day:02}T{curtime.hour:02}:{curtime.minute:02}:{curtime.second:02}Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz")
    for curtime in all_times
]

# import multiprocessing

# print()
# breakpoint()


while start_date <= end_date:
    # prefix = (
    #     os.path.join(
    #         LAMP,
    #         "delta",
    #         start_date.strftime("%Y"),
    #         start_date.strftime("%m"),
    #         start_date.strftime("%d"),
    #     )
    #     + "/"
    # )

    # file_list = file_list_from_s3(
    #     S3_ARCHIVE,
    #     prefix,
    #     in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
    # )

    print(len(file_list))

    breakpoint()

    converter = GtfsRtAdHocConverter(
        config_type=ConfigType.RT_TRIP_UPDATES,
        metadata_queue=None,
        output_location="/Users/hhuang/lamp/gtfs-rt-continuous",
        polars_filter=pl.col("trip_update.trip.route_id").is_in(
            ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
        ),
        max_workers=22,
    )
    converter.add_files(file_list)
    converter.convert()

    # Define the path to your input Parquet files (can use a glob pattern)
    input_path = f"/Users/hhuang/lamp/gtfs-rt-continuous/lamp/RT_TRIP_UPDATES/year={start_date.year}/month={start_date.month}/day={start_date.day}/"
    # output_file = "/tmp/combined_file.parquet"
    output_file2 = f"/Users/hhuang/dataset/{start_date.year}_{start_date.month}_{start_date.day}.parquet"

    # Create a dataset from the input files
    ds = pd.dataset(input_path, format="parquet")

    ## experiment 1 - 2gb. exploded. Why?
    # Read the dataset and write it to a single Parquet file
    # pd.write_dataset(ds, output_file)
    # pq.ParquetWriter(hash_pq_path, schema=out_ds.schema,

    # # experiment 2 - no compression. 1.3gb
    # with pq.ParquetWriter(output_file2, schema=ds.schema) as writer:
    #     for batch in ds.to_batches(batch_size=512 * 1024):
    #         writer.write_batch(batch)

    # experiment 3 - compression. 600mb
    with pq.ParquetWriter(output_file2, schema=ds.schema, compression="zstd", compression_level=3) as writer:
        for batch in ds.to_batches(batch_size=512 * 1024):
            writer.write_batch(batch)

    start_date = start_date + timedelta(days=1)

#     os.path.join(BASE_DIR, FILE_TEMPLATE.format(timestamp=time_obj.strftime('%Y-%m-%d_%H-%M-%S')))
#     for time_obj in hourly_times
# ]

# files = file_list_from_s3_date_range(bucket_name=S3_ARCHIVE, file_prefix=bucket_filter, path_template=path_template, start_date=start_date, end_date=start_date)


# date time range path builder - faster than listing..86k max. (if per second...which it is < than)
# try/except for times that dont exist


# logger.add_metadata(files=len(files))

# with open("cache_files.txt", "w") as file:
#     for item in files:
#         file.write(f"{item}\n")

# breakpoint()
# grouped_files = group_sort_file_list(files)

# grou

# re-construct springboard file (rail and light rail)

# process under tableau

# upload
