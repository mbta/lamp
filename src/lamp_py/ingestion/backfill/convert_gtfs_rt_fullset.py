# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from concurrent.futures import ThreadPoolExecutor
import logging
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from queue import Queue
import tempfile
from typing import Dict, Iterable, List, Optional, Tuple
import pyarrow
from pyarrow import fs
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
import pyarrow.compute as pc


# pylint disable=too-many-arguments
class GtfsRtTripsFullSetConverter(GtfsRtConverter):
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

                self.write_local_pq_partition(table, local_path)

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

    def write_local_pq_partition(self, table: pyarrow.Table, local_path: str) -> None:
        """
        just write the file out..

        # this should be roughly sorted by timestamp.
        """

        if self.filter is not None:
            table = pl.from_arrow(table).filter(self.filter).to_arrow()

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

    def partition_column(self) -> str:
        return "trip_update.trip.route_id"

    def table_sort_order() -> List[Tuple[str, str]]:
        return [
            ("feed_timestamp", "ascending"),
            ("trip_update.trip.route_pattern_id", "ascending"),
            ("trip_update.trip.direction_id", "ascending"),
            ("trip_update.vehicle.id", "ascending"),
        ]
