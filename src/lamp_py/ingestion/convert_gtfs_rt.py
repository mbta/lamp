import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import (
    datetime,
)
from queue import Queue
from threading import current_thread
from typing import Iterator, List, Optional

import dataframely as dy
import msgspec
import polars as pl
from pyarrow import fs

from lamp_py.aws.s3 import (
    move_s3_objects,
)
from lamp_py.ingestion.config_busloc_trip import RtBusTripDetail
from lamp_py.ingestion.config_busloc_vehicle import BusLocVehicleDetail
from lamp_py.ingestion.config_rt_alerts import RtAlertsDetail
from lamp_py.ingestion.config_rt_trip import RtTripDetail
from lamp_py.ingestion.config_rt_vehicle import RtVehicleDetail
from lamp_py.ingestion.converter import ConfigType, Converter
from lamp_py.ingestion.gtfs_rt_detail import GTFSRTDetail
from lamp_py.ingestion.gtfs_rt_structs import FeedMessage, GTFSRealtimeTable
from lamp_py.runtime_utils.lamp_exception import NoImplException
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import LAMP, S3_ARCHIVE, S3_ERROR
from lamp_py.utils.typing import struct_to_schema


class VehiclePositions(dy.Schema):
    """Structured VehiclePositions message."""

    entity = dy.List(
        inner=dy.Struct(
            {
                "id": dy.String(primary_key=True),
                "vehicle": dy.Struct(
                    inner={
                        "trip": dy.Struct(
                            inner={
                                "trip_id": dy.String(nullable=True),
                                "route_id": dy.String(nullable=True),
                                "direction_id": dy.Int8(min=0, max=1, nullable=True),
                                "start_time": dy.String(nullable=True),
                                "start_date": dy.String(nullable=True),
                                "revenue": dy.Bool(nullable=True),
                                "last_trip": dy.Bool(nullable=True),
                                "schedule_relationship": dy.String(nullable=True),
                            }
                        ),
                        "vehicle": dy.Struct(
                            inner={
                                "id": dy.String(nullable=True),
                                "label": dy.String(nullable=True),
                            }
                        ),
                        "position": dy.Struct(
                            inner={
                                "bearing": dy.UInt16(nullable=True),
                                "latitude": dy.Float64(nullable=True),
                                "longitude": dy.Float64(nullable=True),
                                "speed": dy.Float64(nullable=True),
                            }
                        ),
                        "current_stop_sequence": dy.Int16(nullable=True),
                        "stop_id": dy.String(nullable=True),
                        "timestamp": dy.Int64(nullable=True),
                        "occupancy_status": dy.String(nullable=True),
                        "occupancy_percentage": dy.UInt32(nullable=True),
                        "current_status": dy.String(nullable=True),
                    }
                ),
            },
            alias="vehicle",
        ),
        nullable=False,
    )


class GtfsRtConverter(Converter):
    """
    Converter that handles GTFS Real Time JSON data

    https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz
    https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz
    https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_TripUpdates_enhanced.json.gz
    https_mbta_busloc_s3.s3.amazonaws.com_prod_VehiclePositions_enhanced.json.gz
    https_mbta_integration.mybluemix.net_vehicleCount.gz
    """

    def __init__(self, config_type: ConfigType, metadata_queue: Queue[Optional[str]]) -> None:
        Converter.__init__(self, config_type, metadata_queue)

        # Depending on filename, assign self.details to correct implementation
        # of GTFSRTDetail class.
        self.detail: GTFSRTDetail

        if config_type == ConfigType.RT_ALERTS:
            self.detail = RtAlertsDetail()
        elif config_type == ConfigType.RT_TRIP_UPDATES:
            self.detail = RtTripDetail()
        elif config_type == ConfigType.RT_VEHICLE_POSITIONS:
            self.detail = RtVehicleDetail()
        elif config_type == ConfigType.BUS_VEHICLE_POSITIONS:
            self.detail = BusLocVehicleDetail()
        elif config_type == ConfigType.BUS_TRIP_UPDATES:
            self.detail = RtBusTripDetail()
        elif config_type == ConfigType.DEV_GREEN_RT_TRIP_UPDATES:
            self.detail = RtTripDetail()
        elif config_type == ConfigType.DEV_GREEN_RT_VEHICLE_POSITIONS:
            self.detail = RtVehicleDetail()
        else:
            raise NoImplException(f"No Specialization for {config_type}")

        self.error_files: List[str] = []
        self.archive_files: List[str] = []
        self.max_files_to_convert = 60
        self.fs: fs.FileSystem = fs.LocalFileSystem()

    def convert(self) -> None:
        """Destructure files, validate against schema, and append to remote parquet."""
        process_logger = ProcessLogger(
            "convert",
            table_type="gtfs-rt",
            config_type=str(self.config_type),
        )
        process_logger.log_start()

        self.files = self.files[: self.max_files_to_convert]

        process_logger.add_metadata(file_count=len(self.files))

        self.get_filesystem()

        date_range = self.calculate_date_range(self.files)

        records_file = self.encode_records_as_ndjson(self.get_records())

        entities = self.scan_ndjson(records_file)

        try:
            new_table = self.detail.flatten_record(entities)

            existing_table = self.get_existing_table(date_range)

            valid = self.detail.table_schema.validate(pl.concat([existing_table, new_table]), eager=False, cast=True)

            self.write(valid)

            self.archive_files += self.files

        except Exception as exception:
            self.error_files += self.files
            process_logger.log_failure(exception)
        else:
            process_logger.log_complete()
        finally:
            self.move_s3_files()
            os.unlink(records_file)

    def move_s3_files(self) -> None:
        """Move archive and error files to their respective s3 buckets."""
        if len(self.error_files) > 0:
            self.error_files = move_s3_objects(
                self.error_files,
                os.path.join(S3_ERROR, LAMP),
            )

        if len(self.archive_files) > 0:
            self.archive_files = move_s3_objects(
                self.archive_files,
                os.path.join(S3_ARCHIVE, LAMP),
            )

    def write(self, unioned_lf: dy.LazyFrame[GTFSRealtimeTable]) -> bool:
        """Sink existing and new records to s3, partitioned by year, month, day."""
        process_logger = ProcessLogger("GtfsRtConverter.write", config_type=str(self.config_type))
        self.detail.table_schema.sink_parquet(
            unioned_lf,
            pl.PartitionBy(
                self.detail.remote_location.s3_uri,
                file_path_provider=output_path,
                key=[
                    pl.from_epoch("feed_timestamp").dt.date().alias("date"),
                ],
                include_key=False,
            ),
            compression="zstd",
            compression_level=3,
        )
        process_logger.log_complete()

        return True

    def get_existing_table(self, date_range: set) -> pl.LazyFrame:
        """Scan existing table from s3. If no existing table, create empty table with correct schema."""
        process_logger = ProcessLogger("get_existing_table")
        paths = [
            os.path.join(
                self.detail.remote_location.s3_uri,
                f"year={date.year}",
                f"month={date.month}",
                f"day={date.day}",
                "*.parquet",
            )
            for date in date_range
        ]
        process_logger.add_metadata(paths=paths)
        process_logger.log_start()
        try:
            lf = pl.scan_parquet(paths, schema=self.detail.table_schema.to_polars_schema())
            process_logger.log_complete()
            return lf
        except FileNotFoundError:
            return self.detail.table_schema.create_empty(lazy=True)
        except dy.exc.ValidationError as e:
            process_logger.log_failure(e)
            return self.detail.table_schema.create_empty(lazy=True)

    def calculate_date_range(self, files: List[str]) -> set:
        """
        Calculate date range from list of files. Assumes files are named in format {timestamp}_enhanced.json.gz and timestamp is in ISO format with timezone.

        Args:
            files (List[str]): List of file paths.

        Returns:
            set: Set of unique dates in the file list.
        """
        return set(
            datetime.strptime(file.split("/")[-1].split("_")[0], "%Y-%m-%dT%H:%M:%S%z").date()
            for file in [files[0], files[-1]]
        )

    def get_filesystem(self) -> None:
        """Update the converter's filesystem to S3 if files are in S3."""
        if self.files and self.files[0].startswith("s3://"):
            self.fs = fs.S3FileSystem()
        else:
            self.fs = fs.LocalFileSystem()

    def thread_init(self) -> None:
        """Initialize the thread using the shared filesystem."""
        thread_data = current_thread()
        thread_data.__dict__["file_system"] = self.fs

    def validate_record(
        self, filename: str, decoder: msgspec.json.Decoder
    ) -> tuple[str, FeedMessage | msgspec.DecodeError]:
        """Validate a single JSON using the config-defined schema."""
        try:
            with self.fs.open_input_stream(filename, compression="detect") as f:
                record = decoder.decode(f.read())
        except msgspec.DecodeError as e:
            record = msgspec.DecodeError(f"Failed to decode {filename}: {str(e)}")

        return filename, record

    def get_records(self) -> Iterator[FeedMessage]:
        """Process files in parallel for S3 I/O."""
        process_logger = ProcessLogger("validate_records", config_type=str(self.config_type))
        decoder = msgspec.json.Decoder(self.detail.record_schema)

        with ThreadPoolExecutor(max_workers=15, initializer=self.thread_init) as pool:
            for filename, record in pool.map(lambda f: self.validate_record(f, decoder), self.files[:]):
                if isinstance(record, msgspec.DecodeError):
                    process_logger.log_failure(record)
                    self.error_files.append(filename)
                else:
                    yield record

        # Update self.files to remove errors
        self.files = [f for f in self.files if f not in self.error_files]
        process_logger.log_complete()

    def encode_records_as_ndjson(self, records: Iterator[FeedMessage]) -> str:
        """Stream records to a temporary NDJSON file and return the path."""
        encoder = msgspec.json.Encoder()
        buffer = bytearray(4 * 1024 * 1024)  # 4MB buffer
        write_buffer = bytearray()

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson", delete=False) as temp_file:
            try:
                for record in records:
                    encoder.encode_into(record, buffer)
                    write_buffer.extend(buffer)
                    write_buffer.extend(b"\n")

                    # Batch writes at 10MB
                    if len(write_buffer) >= 10 * 1024 * 1024:
                        temp_file.write(write_buffer)
                        write_buffer.clear()

                # Write remaining
                if write_buffer:
                    temp_file.write(write_buffer)

                temp_file.close()
                return temp_file.name
            except Exception:
                temp_file.close()
                os.unlink(temp_file.name)
                raise

    def scan_ndjson(self, ndjson_path: str) -> pl.LazyFrame:
        """Scan NDJSON file as a Polars LazyFrame."""
        lf = (
            pl.scan_ndjson(ndjson_path, schema=struct_to_schema(self.detail.record_schema).to_polars_schema())
            .select("entity", pl.col("header").struct.field("timestamp").alias("feed_timestamp"))
            .explode("entity")
            .unnest("entity")
            .unnest(separator=".")
            .unnest(separator=".")
        )

        return lf


def output_path(provider_args: pl.io.partition.FileProviderArgs) -> str:
    """Format the file path given partition keys of a date."""
    date = provider_args.partition_keys.item(0, 0)
    return f"year={date.year}/month={date.month}/day={date.day}/{date.isoformat()}T00:00:00.parquet"
