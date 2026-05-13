# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from concurrent.futures import Future, ThreadPoolExecutor
import logging
import os
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Dict, Iterable, List, Optional, Tuple
import pyarrow
import pyarrow.parquet as pq
import polars as pl

from lamp_py.aws.s3 import move_s3_objects, upload_file
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3_ERROR, S3_SPRINGBOARD, S3Location

import json


# pylint disable=too-many-arguments
class GtfsRtFullPartitionConverter(GtfsRtConverter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    """

    def __init__(
        self,
        config_type: ConfigType,
        metadata_queue: Queue[Optional[str]],
        local_output_location: str = "/tmp/gtfs-rt-fullset/",
        remote_output_location: S3Location | None = None,
        polars_filter: pl.Expr = pl.lit(True),  # default to true - which will essentially not filter
        max_workers: int = 8,
        time_chunk_minutes: int = 15,
        move_source_on_completion: bool = False,
    ) -> None:
        GtfsRtConverter.__init__(
            self, config_type, metadata_queue, max_workers=max_workers, time_chunk_minutes=time_chunk_minutes
        )

        if time_chunk_minutes < 5:
            raise ValueError(
                "time_chunk_minutes must be at least 5 to ensure proper partitioning and avoid too many small files"
            )
        self.detail = RtTripDetail()
        self.tmp_folder = local_output_location
        self.remote_output_location = remote_output_location
        self.data_parts: Dict[datetime, TableData] = {}
        self.filter = polars_filter
        self.move_source_on_completion = move_source_on_completion

    def convert(self) -> None:
        """
        main convert function - will convert all files in self.files to parquet and
        upload to s3 if remote_output_location is provided

        Specialization over GtfsRtConverter::convert() - this converter does not apply unique()
        on the incoming GTFS-RT data, and will partition files by day or by time chunk (e.g. 15 min intervals)
        depending on the time_chunk_minutes parameter. it will also apply a polars filter to the data at the
        point of conversion from json.gz to pyarrow table, which should help reduce the amount of data being
        written out and speed up the conversion process. This converter is applicable for live ingestion and
        backfill tasks.
        """
        process_logger = ProcessLogger(
            "fullset_gtfs_parquet_table_creator",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
            file_count=len(self.files),
        )
        process_logger.log_start()

        table_count = 0
        move_futures: List[Future] = []
        move_executor = ThreadPoolExecutor(max_workers=1) if self.move_source_on_completion else None
        try:
            for table, partition_dt in self.process_files():
                if table.num_rows == 0:
                    continue

                path_suffix = os.path.join(
                    LAMP,
                    str(self.config_type),
                    f"year={partition_dt.year}",
                    f"month={partition_dt.month}",
                    f"day={partition_dt.day}",
                    f"{partition_dt.isoformat()}.parquet",
                )

                local_path = os.path.join(self.tmp_folder, path_suffix)

                os.makedirs(Path(local_path).parent, exist_ok=True)

                self.write_local_pq_partition(table, local_path)

                # in backfill mode, we don't want to move files around in s3 -
                # we just want to write the converted files to the output location
                # (which could be a temp location or the final archive location).
                # in non-backfill mode, we want to move the original gtfs-rt files from
                # incoming to archive after successful conversion.
                if self.move_source_on_completion and move_executor is not None:
                    # snapshot and clear file lists so the async move
                    # doesn't interfere with ongoing accumulation
                    archive_snapshot = self.archive_files[:]
                    error_snapshot = self.error_files[:]
                    self.archive_files = []
                    self.error_files = []
                    move_futures.append(
                        move_executor.submit(self._move_s3_files_async, archive_snapshot, error_snapshot)
                    )

                # mirror on s3 if remote output location is provided
                if self.remote_output_location is not None:
                    s3_path = os.path.join(self.remote_output_location.s3_uri, path_suffix)
                    upload_file(local_path, s3_path)

                pool = pyarrow.default_memory_pool()
                pool.release_unused()
                table_count += 1
                process_logger.add_metadata(table_count=table_count)

            # wait for all background s3 moves to finish
            for future in move_futures:
                future.result()

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.data_parts = {}
            if move_executor is not None:
                move_executor.shutdown(wait=False)

    @staticmethod
    def _move_s3_files_async(archive_files: List[str], error_files: List[str]) -> None:
        """Move archive and error files to S3 in a background thread."""
        if error_files:
            move_s3_objects(error_files, os.path.join(S3_ERROR, LAMP))
        if archive_files:
            move_s3_objects(archive_files, os.path.join(S3_ARCHIVE, LAMP))

    def write_local_pq_partition(self, table: pyarrow.Table, local_path: str) -> None:
        """
        just write the file out..

        this should be already sorted by timestamp based on how self.files is yielded.
        """

        table = pl.from_arrow(table).filter(self.filter).to_arrow()

        # read local_path if exists and concat with table
        # this handles the case where a prior iteration yielded an incomplete time chunk,
        # and we are now filling in the remaining record that is part of that chunk
        if os.path.exists(local_path):
            existing_table = pq.read_table(local_path)
            table = pyarrow.concat_tables([existing_table, table])

        writer = pq.ParquetWriter(local_path, schema=table.schema, compression="zstd", compression_level=3)
        writer.write_table(table)
        writer.close()

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        iterate through all of the files to be converted - apply a polars filter to narrow results at the source to reduce write churn - filter at json.gz input level
        convert to pyarrow tables, group into time chunk intervals, and yield tables for completed intervals as we go
        """

        process_logger = ProcessLogger(
            "fullset_create_pyarrow_tables",
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

                # create key for self.data_parts dictionary that bins based on the chunk interval
                dt_part = self._interval_key(result_dt)

                # create new self.table_groups entry for key if it doesn't exist
                if dt_part not in self.data_parts:
                    self.data_parts[dt_part] = TableData()
                    self.data_parts[dt_part].table = self.detail.transform_for_write(rt_data)
                else:
                    self.data_parts[dt_part].table = pyarrow.concat_tables(
                        [self.data_parts[dt_part].table, self.detail.transform_for_write(rt_data)]
                    )

                self.data_parts[dt_part].files.append(result_filename)

                # we're mapping each gz to a range dt_part. when we have > 1 dt_parts in the map,
                # that means we've processed files from at least 2 different time intervals and
                # can start yielding tables for the intervals that are complete.
                # this relies on self.files being sorted - enforced by add_files(),
                # and ThreadPoolExecutor/map yields __next__ iterator, i.e. returns in the right order
                if len(self.data_parts) > 1:
                    yield from self.yield_check_periodic(process_logger, result_dt)

        yield from self.yield_check_periodic(process_logger, flush=True)

        process_logger.add_metadata(file_count=0, number_of_rows=0)
        process_logger.log_complete()

    def partition_column(self) -> str:
        return "trip_update.trip.route_id"

    def table_sort_order(self) -> List[Tuple[str, str]]:
        return [
            ("feed_timestamp", "ascending"),
            ("trip_update.trip.route_pattern_id", "ascending"),
            ("trip_update.trip.direction_id", "ascending"),
            ("trip_update.vehicle.id", "ascending"),
        ]

    def _interval_key(self, ts: datetime) -> datetime:
        """
        Truncate a UTC datetime to its wall-clock-aligned interval start.

        For time_chunk_minutes=15:
            01:07 -> 01:00,  01:15 -> 01:15,  01:29 -> 01:15,  23:59 -> 23:45

        Returns a naive datetime (no timezone) matching the existing data_parts key convention.
        """
        total_minutes = ts.hour * 60 + ts.minute
        aligned_minutes = (total_minutes // self.time_chunk_minutes) * self.time_chunk_minutes
        return datetime(
            year=ts.year,
            month=ts.month,
            day=ts.day,
            hour=aligned_minutes // 60,
            minute=aligned_minutes % 60,
        )

    def yield_check_periodic(
        self,
        process_logger: ProcessLogger,
        current_ts: datetime = datetime.now(),
        flush: bool = False,
    ) -> Iterable[Tuple[pyarrow.table, datetime]]:
        """
        Yield completed time-chunk intervals from data_parts.

        When processing files in reverse chronological order, an interval is
        complete once current_ts is *before* the interval start (we've moved
        past it going backwards and won't see more data for it).

        When flush=True, yield all remaining intervals regardless of current_ts.

        @current_ts - the feed timestamp of the file just processed
        @flush - if True, yield everything remaining
        """
        current_interval = self._interval_key(current_ts)
        for iter_ts in list(self.data_parts.keys()):
            table = self.data_parts[iter_ts].table
            if table is None:
                continue

            # yield if we've moved past this interval (reverse order)
            # or if flushing all remaining data
            if flush or current_interval < iter_ts:

                # only populate archive_files if we're in live ingestion mode
                if self.move_source_on_completion:
                    self.archive_files += self.data_parts[iter_ts].files

                process_logger.add_metadata(
                    file_count=len(self.data_parts[iter_ts].files),
                    number_of_rows=table.num_rows,
                    interval_start=iter_ts.isoformat(),
                )
                process_logger.log_complete()
                process_logger.add_metadata(file_count=0, number_of_rows=0, print_log=False)
                process_logger.log_start()

                yield (table, iter_ts)

                del self.data_parts[iter_ts]
