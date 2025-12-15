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
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, TABLEAU, S3Location
import polars as pl

from lamp_py.tableau.conversions import convert_gtfs_rt_trip_updates
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob, days_ago
from lamp_py.tableau.jobs.lamp_jobs import GTFS_RT_TABLEAU_PROJECT

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


class GtfsRtTripsAdHocConverter(GtfsRtConverter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    """

    def __init__(
        self,
        config_type: ConfigType,
        metadata_queue: Queue[Optional[str]],
        output_location: str,
        polars_filter: pl.Expr | bool = True, # default to true - which will essentially not filter
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
                if result_dt is None:
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

def delta_reingestion_runner(start_date, end_date, final_output_base, polars_filter = None, max_workers = 4, tmp_output_location = "/tmp/gtfs-rt-continuous/"):

    logger = ProcessLogger("backfiller")
    logger.log_start()
    while start_date <= end_date:
        prefix = (
            os.path.join(
                LAMP,
                "delta",
                start_date.strftime("%Y"),
                start_date.strftime("%m"),
                start_date.strftime("%d"),
            )
            + "/"
        )

        file_list = file_list_from_s3(
            S3_ARCHIVE,
            prefix,
            in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
        )

        print(len(file_list))

        converter = GtfsRtTripsAdHocConverter(
            config_type=ConfigType.RT_TRIP_UPDATES,
            metadata_queue=None,
            output_location=tmp_output_location,
            polars_filter=polars_filter,
            max_workers=max_workers,
        )
        converter.add_files(file_list)
        converter.convert()

        # Define the path to your input Parquet files (can use a glob pattern)
        input_path = f"{output_location}/lamp/RT_TRIP_UPDATES/year={start_date.year}/month={start_date.month}/day={start_date.day}/"
        # output_file = "/tmp/combined_file.parquet"
        output_path = f"{final_output_base}/{start_date.year}_{start_date.month}_{start_date.day}.parquet"

        # Create a dataset from the input files
        ds = pd.dataset(input_path, format="parquet")

        # experiment 3 - compression. 600mb
        with pq.ParquetWriter(output_path, schema=ds.schema, compression="zstd", compression_level=3) as writer:
            for batch in ds.to_batches(batch_size=512 * 1024):
                writer.write_batch(batch)                   

        start_date = start_date + timedelta(days=1)

    # now combine them

    tableau_adhoc_output = S3Location(
        bucket=S3_ARCHIVE,
        prefix=os.path.join("adhoc", TABLEAU, "rail"),
    )
    
    hyperjob = FilteredHyperJob(
        remote_input_location=final_output_base,
        remote_output_location=tableau_adhoc_output,
        start_date=start_date,
        end_date=end_date,
        processed_schema=convert_gtfs_rt_trip_updates.TripUpdates.pyarrow_schema(),
        dataframe_filter=select_only_required,
        parquet_filter=None,
        tableau_project_name=GTFS_RT_TABLEAU_PROJECT,
    )

    hyperjob.run_parquet()
    # hyperjob.run_hyper()


    # trip_update.trip.trip_id
    # trip_update.trip.start_date
    # trip_update.stop_time_update.departure.time - Convert from Epoch format to regular datetime
    # feed_timestamp - Convert from Epoch format to regular datetime
    # Filter data to trip_update.trip.revenue = TRUE, trip_update.stop_time_update.schedule_relationship != SKIPPED, and trip_update.trip.schedule_relationship != CANCELED        

def select_only_required(df: pl.DataFrame) -> pl.DataFrame:
    # Filter data to  = TRUE, trip_update.stop_time_update.schedule_relationship != SKIPPED, and trip_update.trip.schedule_relationship != CANCELED        

    df = df.filter(
            # pl.col("trip_update.stop_time_update.departure.time").is_not_null(),
            # pl.col("trip_update.stop_time_update.stop_id").is_in(LightRailFilter.terminal_stop_ids),
            pl.col("trip_update.trip.revenue"),
            pl.col("trip_update.trip.schedule_relationship").ne("CANCELED"),
            pl.col("trip_update.stop_time_update.schedule_relationship").ne("SKIPPED"),
            # pl.col("trip_update.stop_time_update.departure.time").sub(pl.col("feed_timestamp")).dt.total_seconds().ge(1),
        ).select([
            "trip_update.trip.trip_id"
            "trip_update.trip.start_date"
            "trip_update.stop_time_update.departure.time" #  - Convert from Epoch format to regular datetime
            "feed_timestamp" # - Convert from Epoch format to regular datetime
            ])
    # 
if __name__ == "__main__":

    start_date = datetime(2025, 12, 1, 0, 0, 0)
    end_date = datetime(2025, 12, 2, 0, 0, 0)
    output_location="/Users/hhuang/lamp/gtfs-rt-continuous"
    polars_filter = pl.col("trip_update.trip.route_id").is_in(
                ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
            )
    max_workers = 22
    final_output_base = "/Users/hhuang/dataset"
    delta_reingestion_runner(start_date=start_date, end_date=end_date, tmp_output_location=output_location, final_output_base=final_output_base, polars_filter=polars_filter, max_workers=22)
