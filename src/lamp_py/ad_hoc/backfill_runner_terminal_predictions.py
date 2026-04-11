# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from queue import Queue
from typing import Dict, Iterable, List, Optional
import pyarrow
import pyarrow.dataset as pd
import pyarrow.parquet as pq
import dataframely as dy
import polars as pl

from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.aws.s3 import file_list_from_s3, upload_file
from lamp_py.runtime_utils.remote_files import springboard_rt_vehicle_positions
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3Location

from lamp_py.tableau.conversions import convert_gtfs_rt_vehicle_position
from lamp_py.tableau.conversions.convert_gtfs_rt_trip_updates import filter_valid_devgreen_terminal_predictions
from lamp_py.tableau.jobs.filtered_hyper import FilteredHyperJob
from lamp_py.tableau.jobs.lamp_jobs import GTFS_RT_TABLEAU_PROJECT
from lamp_py.utils.filter_bank import FilterBankRtVehiclePositions, HeavyRailFilter, LightRailFilter
import json

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


# pylint disable=too-many-arguments
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
        polars_filter: pl.Expr | None = None,  # default to true - which will essentially not filter
        max_workers: int = 4,
        verbose: bool = False,
    ) -> None:
        GtfsRtConverter.__init__(self, config_type, metadata_queue, max_workers=max_workers)

        self.detail = RtTripDetail()

        self.tmp_folder = output_location

        self.data_parts: Dict[datetime, TableData] = {}

        self.error_files: List[str] = []
        self.archive_files: List[str] = []
        self.table_stats = {}

        self.verbose = verbose
        if polars_filter is None:
            polars_filter = pl.lit(None)
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
            stats_file = os.path.join(self.tmp_folder, "table_stats.json")
            with open(stats_file, "a") as f:
                json.dump(self.table_stats, f, indent=2, default=str)
        finally:
            self.data_parts = {}

    def write_local_pq(self, table: pyarrow.Table, local_path: str) -> None:
        """
        just write the file out..

        # this should be roughly sorted by timestamp.
        """
        print("running GtfsRtTripUpdatesConverter::write_local_pq")

        # self.table_stats[local_path] = table.parquet.RowGroupMetaData.to_dict()

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
            # for file in self.files:
            #     result_dt, result_filename, rt_data = self.gz_to_pyarrow(file)

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
                    self.data_parts[dt_part].table = self.detail.transform_for_write(rt_data)

                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [self.data_parts[dt_part].table, self.detail.transform_for_write(rt_data)]
                    )

                self.data_parts[dt_part].files.append(result_filename)

                yield from self.yield_check(process_logger)

        # yield any remaining tables
        yield from self.yield_check(process_logger, min_rows=-1)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()


def delta_reingestion_runner(
    start_date: date,
    end_date: date,
    final_output_path: S3Location,
    polars_filter: pl.Expr | None = None,
    max_workers: int = 4,
    local_output_location: str = "/tmp/gtfs-rt-continuous/",
) -> None:
    """
    Docstring for delta_reingestion_runner

    :param start_date: start of reingestion range
    :type start_date: date
    :param end_date: end of reingestion range
    :type end_date: date
    :param final_output_base: final resting prefix-path of output artifacts
    :type final_output_base: S3Location
    :param polars_filter: optional polars expression filter to be applied
    :type polars_filter: pl.Expr | None
    :param max_workers: number of worker threads to ingest delta files with - default is 4
    :type max_workers: int
    :param local_output_location: temporary working directory to place outputs
    :type local_output_location: str

    """
    logger = ProcessLogger("backfiller")
    logger.log_start()

    cur_date = start_date

    while cur_date <= end_date:
        prefix = (
            os.path.join(
                LAMP,
                "delta",
                cur_date.strftime("%Y"),
                cur_date.strftime("%m"),
                cur_date.strftime("%d"),
            )
            + "/"
        )

        # check local cache of file_list - this is only valid for reingestion because archive/delta is static for past days
        if end_date.date() < datetime.now().date():
            cache_file = f"file_list_{cur_date.isoformat()}.txt"
            if os.path.exists(cache_file):
                with open(cache_file, "r") as f:
                    file_list = [line.strip() for line in f.readlines()]
            else:
                file_list = file_list_from_s3(
                    S3_ARCHIVE,
                    prefix,
                    in_filter="mbta.com_realtime_TripUpdates_enhanced.json.gz",
                )
                with open(cache_file, "w") as f:
                    for file in file_list:
                        f.write(f"{file}\n")

        print(len(file_list))

        #### Stage 1: local to local (MANY to many)

        # construct and run converter once per day
        converter = GtfsRtTripsAdHocConverter(
            config_type=ConfigType.RT_TRIP_UPDATES,
            metadata_queue=Queue(),
            output_location=local_output_location,
            polars_filter=polars_filter,
            max_workers=max_workers,
            verbose=True,
        )
        converter.add_files(file_list)
        # this outputs to local output_location=tmp_output_location
        converter.convert()

        ## Stage 2: local to local (many to 1)

        # Define the path to your input Parquet files (can use a glob pattern)
        converter_output_path = f"{local_output_location}/lamp/RT_TRIP_UPDATES/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/"
        consolidated_parquet_output_file = (
            f"{local_output_location}/{cur_date.year}_{cur_date.month}_{cur_date.day}.parquet"
        )

        # Create a dataset from the input files
        ds = pd.dataset(converter_output_path, format="parquet")

        with pq.ParquetWriter(
            consolidated_parquet_output_file, schema=ds.schema, compression="zstd", compression_level=3
        ) as writer:
            for batch in ds.to_batches(batch_size=512 * 1024):
                writer.write_batch(batch)

        #### Stage 3: local to remote (one to one)

        # upload local to remote
        upload_file(
            consolidated_parquet_output_file,
            consolidated_parquet_output_file.replace(
                f"{local_output_location}/",
                f"{final_output_path.s3_uri}/year={cur_date.year}/month={cur_date.month}/day={cur_date.day}/",
            ),
        )

        cur_date = cur_date + timedelta(days=1)

        converter.clean_local_folders()  # clear local output folders after each day to manage disk space


def run_backfill() -> None:
    """
    Full encapsulated method to call all of this backfill job
    """
    local_tmp_output = "/tmp/gtfs-rt-continuous/"

    if not os.path.exists(local_tmp_output):
        os.makedirs(local_tmp_output)

    start = datetime(2026, 3, 1, 0, 0, 0)
    end = datetime(2026, 3, 29, 0, 0, 0)

    all_terminal_stops = LightRailFilter.terminal_stop_ids + HeavyRailFilter.terminal_stop_ids
    polars_filter = pl.col("trip_update.trip.route_id").is_in(
        ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E", "Mattapan"]
    ) & pl.col("trip_update.stop_time_update.stop_id").is_in(all_terminal_stops)

    final_output_path = S3Location(S3_ARCHIVE, "lamp/adhoc/RT_TRIP_UPDATES_RAIL_TERMINAL")

    delta_reingestion_runner(
        start_date=start,
        end_date=end,
        local_output_location=local_tmp_output,
        final_output_path=final_output_path,
        polars_filter=polars_filter,
        max_workers=4,
    )


if __name__ == "__main__":
    run_backfill()
