# pylint: disable=too-many-positional-arguments,too-many-arguments, too-many-locals, redefined-outer-name, R0801

from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Dict, Iterable, List, Optional, Tuple
import pyarrow
import polars as pl

from lamp_py.aws.s3 import upload_file
from lamp_py.ingestion.convert_gtfs_rt import GtfsRtConverter, TableData
from lamp_py.ingestion.converter import ConfigType

from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3Location


# pylint: disable=too-many-arguments,too-many-instance-attributes
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
        """
        Initialize GTFS-RT fullset converter with time-chunked partitioning.

        Args:
            config_type: Type of GTFS-RT configuration (trip updates, vehicle positions).
            metadata_queue: Queue for metadata communication.
            local_output_location: Local directory for temporary parquet files.
            remote_output_location: S3 location for final output; if None, files stay local.
            polars_filter: Polars expression to filter data at conversion time; defaults to no filtering.
            max_workers: Number of worker threads for parallel processing.
            time_chunk_minutes: Minutes for time-based partitioning (e.g., 15 min intervals).
            move_source_on_completion: If True, move source files to archive after completion.
                For the LAMP usecase, `source` in this context refers to the `delta` or `archive`
                bucket, for ingestion or backfill respectively. The output is uploaded regardless,
                but the `source` is only conditionally moved.
                    `delta` -> `archive` True, do this,
                    `archive` -> `archive` False, don't do this
        """
        GtfsRtConverter.__init__(self, config_type, metadata_queue, max_workers=max_workers)

        if time_chunk_minutes < 5:
            raise ValueError(
                "time_chunk_minutes must be at least 5 to ensure proper partitioning and avoid too many small files"
            )

        # Keep tmp_folder as the base temp root to match GtfsRtConverter helpers
        # (clean_local_folders, continuous_pq_update, etc.), which append
        # lamp/<config_type> internally.
        self.tmp_folder = local_output_location
        self.remote_output_location = remote_output_location
        self.data_parts: Dict[datetime, TableData] = {}
        self.filter = polars_filter
        self.move_source_on_completion = move_source_on_completion
        self.time_chunk_minutes = time_chunk_minutes

    def convert(self) -> None:
        """
        Convert all files in self.files to time-chunked parquet partitions.

        Specialization over GtfsRtConverter::convert() - this converter does not
        apply unique() on the incoming GTFS-RT data, and will partition files by
        day or by time chunk (e.g. 15 min intervals) depending on the
        time_chunk_minutes parameter. It will also apply a polars filter to the
        data at the point of conversion from json.gz to pyarrow table, which
        should help reduce the amount of data being written out and speed up the
        conversion process. This converter is suitable for live ingestion and
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
        try:
            for table, partition_dt in self.process_files():
                if table.num_rows == 0:
                    continue

                path_suffix = os.path.join(
                    f"year={partition_dt.year}",
                    f"month={partition_dt.month}",
                    f"day={partition_dt.day}",
                    f"{partition_dt.isoformat()}.parquet",
                )

                local_path = os.path.join(self.tmp_folder, LAMP, str(self.config_type), path_suffix)

                os.makedirs(Path(local_path).parent, exist_ok=True)

                self.write_local_pq_partition(table, local_path)

                # in backfill mode, we don't want to move files around in s3 -
                # we just want to write the converted files to the output location
                # (which could be a temp location or the final archive location).
                # in non-backfill mode, we want to move the original gtfs-rt files from
                # incoming to archive after successful conversion.
                if self.move_source_on_completion:
                    self.move_s3_files()

                # mirror on s3 if remote output location is provided
                if self.remote_output_location is not None:
                    s3_path = os.path.join(self.remote_output_location.s3_uri, path_suffix)
                    upload_file(local_path, s3_path)

                # try to get pyarrow to limit memory usage after each loop.
                # this is a "ask nicely and pray" move...we can't manage memory directly in python.
                pyarrow.default_memory_pool().release_unused()

                table_count += 1
                process_logger.add_metadata(table_count=table_count)

        except Exception as exception:
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.data_parts = {}
            if self.move_source_on_completion:
                self.move_s3_files()
            self.clean_local_folders()

    def write_local_pq_partition(self, table: pyarrow.Table, local_path: str) -> None:
        """
        Just write the file out..

        this should be already sorted by timestamp based on how self.files is yielded.
        """
        df: pl.DataFrame = pl.from_arrow(table).filter(self.filter)  # type: ignore[arg-type, assignment]

        # read local_path if exists and concat with table
        # this handles the case where a prior iteration yielded an incomplete time chunk,
        # and we are now filling in the remaining record that is part of that chunk
        if os.path.exists(local_path):
            existing_table: pl.DataFrame = pl.read_parquet(local_path)
            df = pl.concat([existing_table, df], how="diagonal")

        df.write_parquet(local_path, compression="zstd", compression_level=3)

    def process_files(self) -> Iterable[pyarrow.table]:
        """
        Yield time-chunked parquet tables from all input files.

        Applies a polars filter to narrow results at the source to reduce write
        churn. Converts json.gz input files to pyarrow tables, groups into time
        chunk intervals, and yields tables for completed intervals as we go.
        """
        process_logger = ProcessLogger(
            "fullset_create_pyarrow_tables",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        with ThreadPoolExecutor(max_workers=self.max_workers, initializer=self.thread_init) as pool:
            for result_dt, result_filename, rt_data in pool.map(self.gz_to_pyarrow, self.files):
                # errors in gtfs_rt conversions are handled in the gz_to_pyarrow
                # function. if one is encountered, the datetime will be none. log
                # the error and move on to the next file.

                if result_dt is None:
                    process_logger.add_metadata(result=result_dt, file=result_filename)
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
        """Return the column name for partitioning output parquet files."""
        return "trip_update.trip.route_id"

    def table_sort_order(self) -> List[Tuple[str, str]]:
        """Return sort order specification for output parquet tables."""
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

        When flush=True, yield all remaining intervals regardless of current_ts.

        @current_ts - the feed timestamp of the file just processed
        @flush - if True, yield everything remaining
        """
        current_interval = self._interval_key(current_ts)
        for iter_ts in list(self.data_parts.keys()):
            table = self.data_parts[iter_ts].table

            # yield if we've moved past this interval
            # or if flushing all remaining data
            if flush or current_interval > iter_ts:
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
