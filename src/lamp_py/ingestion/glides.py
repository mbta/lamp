from typing import Dict, List, Optional, Type
import os
import gzip
from datetime import datetime
import tempfile
from queue import Queue
from json import dump

from abc import ABC, abstractmethod
import dataframely as dy
import polars as pl
import pyarrow

from lamp_py.aws.s3 import download_file, upload_file
from lamp_py.aws.kinesis import KinesisReader
from lamp_py.ingestion.utils import explode_table_column, flatten_table_schema
from lamp_py.utils.dataframely import unnest_columns, with_nullable, extract_pii_columns, drop_pii_columns
from lamp_py.runtime_utils.process_logger import ProcessLogger
from lamp_py.runtime_utils.remote_files import (
    glides_records,
    springboard_glides,
    S3Location,
)

RFC3339_DATE_REGEX = r"^20(?:([1-3][0-9]-[0-1][0-9]-[0-3][0-9]))"  # up to 2039-19-39
RFC3339_DATETIME_REGEX = RFC3339_DATE_REGEX + r"[T ]([0-2][0-9]:[0-5][0-9]:[0-5][0-9](?:\.\d+)?)(Z|[\+-]\d{2}:\d{2})?$"
GTFS_TIME_REGEX = r"^([0-9]{2}):([0-5][0-9]):([0-5][0-9])$"  # clock can be greater than 24 hours

user = dy.Struct(
    {
        "emailAddress": dy.String(metadata={"pii_roles": ["GlidesUserEmail"]}),
        "badgeNumber": dy.String(nullable=True),
    },
    nullable=True,
)

location = dy.Struct({"gtfsId": dy.String(nullable=True), "todsId": dy.String(nullable=True)})

metadata = dy.Struct(
    {
        "location": location,
        "author": user,
        "inputType": dy.String(nullable=True),
        "inputTimestamp": dy.String(nullable=True, regex=RFC3339_DATETIME_REGEX),  # coercable to datetime
    }
)

trip_key = dy.Struct(
    {
        "serviceDate": dy.String(regex=RFC3339_DATE_REGEX),
        "tripId": dy.String(),
        "startLocation": location,
        "endLocation": location,
        "startTime": dy.String(regex=GTFS_TIME_REGEX),
        "endTime": dy.String(regex=GTFS_TIME_REGEX),
        "revenue": dy.String(regex=r"(non)?revenue"),
        "glidesId": dy.String(),
    },
    nullable=True,
)


class GlidesRecord(dy.Schema):
    """Base schema for all Glides records."""

    id = dy.String()
    type = dy.String()
    time = dy.Datetime(  # in %Y-%m-%dT%H:%M:%S%:z format before serialization
        min=datetime(2024, 1, 1), max=datetime(2039, 12, 31), time_unit="ms"  # within Python's serializable range
    )
    source = dy.String()
    specversion = dy.String()
    dataschema = dy.String(nullable=True)


class EditorChangesRecord(GlidesRecord):
    """Edits made by one inspector session."""

    data = dy.Struct(
        {
            "metadata": with_nullable(metadata, True),
            "changes": dy.List(
                dy.Struct(
                    {
                        "type": dy.String(regex=r"start|stop"),
                        "location": location,
                        "editor": user,
                    }
                ),
                min_length=1,
            ),
        }
    )


class OperatorSignInsRecord(GlidesRecord):
    """Operator confirmations of fitness for duty."""

    data = dy.Struct(
        {
            "metadata": metadata,
            "operator": dy.Struct({"badgeNumber": dy.String()}),
            "signedInAt": dy.String(
                nullable=True, regex=RFC3339_DATETIME_REGEX
            ),  # a string coercable to a UTC datetime
            "signature": dy.Struct({"type": dy.String(), "version": dy.Int16()}),
        }
    )


class TripUpdatesRecord(GlidesRecord):
    """New information about trips, such as operator assignments or dropped trips."""

    data = dy.Struct(
        {
            "metadata": metadata,
            "tripUpdates": dy.List(
                dy.Struct(
                    {
                        "previousTripKey": trip_key,
                        "type": dy.String(),
                        "tripKey": trip_key,
                        "comment": dy.String(nullable=True),
                        "startLocation": location,
                        "endLocation": location,
                        "startTime": dy.String(nullable=True),  # can be "unset" string :(
                        "endTime": dy.String(nullable=True),  # can be "unset" string :(
                        "cars": dy.String(nullable=True),  # an array of objects
                        "revenue": dy.String(nullable=True),
                        "dropped": dy.String(nullable=True),
                        "scheduled": dy.String(),  # an object with an array of objects
                    }
                )
            ),
        }
    )


class VehicleTripAssignmentRecord(GlidesRecord):
    """Which vehicle is operating each trip at the current moment."""

    data = dy.Struct(
        {
            "vehicleId": dy.String(),
            "tripKey": dy.Struct(
                {
                    "serviceDate": dy.String(regex=RFC3339_DATE_REGEX),
                    "tripId": dy.String(),
                    "scheduled": dy.String(),
                },
                nullable=True,
            ),
        }
    )


EditorChangesTable: Type[GlidesRecord] = type(
    "EditorChangesTable", (GlidesRecord,), unnest_columns({"data": EditorChangesRecord.data})
)

OperatorSignInsTable: Type[GlidesRecord] = type(
    "OperatorSignInsTable", (GlidesRecord,), unnest_columns({"data": OperatorSignInsRecord.data})
)

TripUpdatesTable: Type[GlidesRecord] = type(
    "TripUpdatesTable", (GlidesRecord,), unnest_columns({"data": TripUpdatesRecord.data})
)

VehicleTripAssignmentTable: Type[GlidesRecord] = type(
    "VehicleTripAssignmentTable", (GlidesRecord,), unnest_columns({"data": VehicleTripAssignmentRecord.data})
)


class GlidesConverter(ABC):  # pylint: disable=too-many-instance-attributes
    """
    Abstract Base Class for Archiving Glides Events
    """

    tmp_dir = "/tmp"

    def __init__(self, base_filename: str, record_schema: Type[GlidesRecord], table_schema: Type[GlidesRecord]) -> None:
        self.base_filename = base_filename
        self.type = self.base_filename.replace(".parquet", "")
        self.record_schema = record_schema
        self.table_schema = table_schema

        self.records: List[Dict] = []

        self.download_remote()

    @property
    def local_path(self) -> str:
        """Get path to downloaded records."""
        return os.path.join(self.tmp_dir, self.base_filename)

    @property
    def general_directory(self) -> str:
        """Get local path to general-access data."""
        return os.path.join(self.tmp_dir, springboard_glides.s3_uri.replace("s3://", ""))

    @property
    def restricted_directory(self) -> str:
        """Get local path to restricted-access data."""
        return os.path.join(self.tmp_dir, springboard_glides.restricted_s3_uri("GlidesUserEmail").replace("s3://", ""))

    @property
    def get_event_schema(self) -> pyarrow.schema:
        """Pyarrow schema for incoming events before flattening"""
        return self.record_schema.to_pyarrow_schema()

    @property
    def get_table_schema(self) -> pyarrow.schema:
        """Pyarrow schema for springboard-ready datasets after flattening."""
        return self.table_schema.to_pyarrow_schema()

    @property
    def get_pii_column_names(self) -> list[str]:
        """Return list of columns containing PII."""
        return [col.name for col in extract_pii_columns(self.table_schema)]

    @property
    @abstractmethod
    def unique_key(self) -> str:
        """Key in record['data'] that is unique to this event type"""

    def download_remote(self) -> None:
        """Download the remote parquet path for appending"""
        if os.path.exists(self.local_path):
            os.remove(self.local_path)

        if self.get_pii_column_names:
            download_file(
                object_path=os.path.join(os.path.relpath(self.restricted_directory, self.tmp_dir), self.base_filename),
                file_name=self.local_path,
            )
        else:
            download_file(
                object_path=os.path.join(os.path.relpath(self.general_directory, self.tmp_dir), self.base_filename),
                file_name=self.local_path,
            )

    @abstractmethod
    def convert_records(self) -> dy.DataFrame[GlidesRecord]:
        """Convert incoming records into a flattened table of records"""

    def append_records(self) -> None:
        """Add incoming records to a local parquet file"""
        process_logger = ProcessLogger(process_name="append_glides_records", type=self.type)
        process_logger.log_start()

        new_dataset = self.convert_records()

        if os.path.exists(self.local_path):
            remote_records = pl.read_parquet(self.local_path)
            joined_ds = pl.union([new_dataset, remote_records], how="diagonal_relaxed")
        else:
            joined_ds = new_dataset

        process_logger.add_metadata(
            new_records=new_dataset.height,
            total_records=joined_ds.height,
        )

        sorted_ds = joined_ds.unique().sort("time")
        valid = process_logger.log_dataframely_filter_results(*self.table_schema.filter(sorted_ds))
        if not valid.is_empty():
            if self.get_pii_column_names:
                role_dir = os.path.join(self.tmp_dir, self.restricted_directory)
                os.makedirs(role_dir, exist_ok=True)
                valid.write_parquet(os.path.join(role_dir, self.base_filename))
                process_logger.add_metadata(pii_data_path=os.path.join(role_dir, self.base_filename))

            general_dir = os.path.join(self.tmp_dir, self.general_directory)
            os.makedirs(general_dir, exist_ok=True)
            drop_pii_columns(valid, self.table_schema).write_parquet(os.path.join(general_dir, self.base_filename))
            process_logger.add_metadata(general_data_path=os.path.join(general_dir, self.base_filename))

        process_logger.log_complete()

    def upload_records(self) -> None:
        """Upload local parquet files to remote storage."""
        for dirpath, _, filenames in os.walk(self.tmp_dir):
            for f in filenames:
                upload_file(
                    file_name=os.path.join(dirpath, f),
                    object_path=os.path.join("s3://", os.path.relpath(dirpath, self.tmp_dir), f),
                )
                os.remove(os.path.join(dirpath, f))


class EditorChanges(GlidesConverter):
    """
    Converter for Editor Change Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.editors_changed.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(
            self,
            base_filename="editor_changes.parquet",
            record_schema=EditorChangesRecord,
            table_schema=EditorChangesTable,
        )

    @property
    def unique_key(self) -> str:
        return "changes"

    def convert_records(self) -> dy.DataFrame[GlidesRecord]:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        editors_table = pyarrow.Table.from_pylist(self.records, schema=self.get_event_schema)
        editors_table = flatten_table_schema(editors_table)
        editors_table = explode_table_column(editors_table, "data.changes")
        editors_table = flatten_table_schema(editors_table)
        editors_dataset = process_logger.log_dataframely_filter_results(
            *EditorChangesTable.filter(pl.DataFrame(editors_table))
        )

        process_logger.log_complete()
        return editors_dataset


class OperatorSignIns(GlidesConverter):
    """
    Converter for Operator Sign In Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.operator_signed_in.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(
            self,
            base_filename="operator_sign_ins.parquet",
            record_schema=OperatorSignInsRecord,
            table_schema=OperatorSignInsTable,
        )

    @property
    def unique_key(self) -> str:
        return "operator"

    def convert_records(self) -> dy.DataFrame[GlidesRecord]:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()
        osi_table = pyarrow.Table.from_pylist(self.records, schema=self.get_event_schema)
        osi_table = flatten_table_schema(osi_table)
        osi_dataset = process_logger.log_dataframely_filter_results(
            *OperatorSignInsTable.filter(pl.DataFrame(osi_table))
        )

        process_logger.log_complete()
        return osi_dataset


class TripUpdates(GlidesConverter):
    """
    Converter for Trip Update Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.trips_updated.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(
            self, base_filename="trip_updates.parquet", record_schema=TripUpdatesRecord, table_schema=TripUpdatesTable
        )

    @property
    def unique_key(self) -> str:
        return "tripUpdates"

    def convert_records(self) -> dy.DataFrame[GlidesRecord]:
        def flatten_multitypes(record: Dict) -> Dict:
            """
            For each update in a record, flatten out the objects in "cars",
            "dropped", and "cars". These objects are poorly structured for our
            needs and can't be strongly typed. Leave them as json strings for
            now and parse them later when analyzing.
            """
            for update in record["data"]["tripUpdates"]:
                # convert these objects into strings if they exist
                for key in ["scheduled", "dropped", "cars"]:
                    try:
                        update[key] = str(update[key])
                    except KeyError:
                        pass

            return record

        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        modified_records = [flatten_multitypes(r) for r in self.records]
        tu_table = pyarrow.Table.from_pylist(modified_records, schema=self.get_event_schema)
        tu_table = flatten_table_schema(tu_table)
        tu_table = explode_table_column(tu_table, "data.tripUpdates")
        tu_table = flatten_table_schema(tu_table)
        tu_dataset = process_logger.log_dataframely_filter_results(*TripUpdatesTable.filter(pl.DataFrame(tu_table)))

        process_logger.log_complete()
        return tu_dataset


class VehicleTripAssignment(GlidesConverter):
    """
    Converter for Vehcile Trip Assignment Events
    https://mbta.github.io/schemas/events/glides/com.mbta.ctd.glides.vehicle_trip_assignment.v1
    """

    def __init__(self) -> None:
        GlidesConverter.__init__(
            self,
            base_filename="vehicle_trip_assignment.parquet",
            record_schema=VehicleTripAssignmentRecord,
            table_schema=VehicleTripAssignmentTable,
        )

    @property
    def unique_key(self) -> str:
        return "tripKey"

    def convert_records(self) -> dy.DataFrame[GlidesRecord]:
        process_logger = ProcessLogger(process_name="convert_records", type=self.type)
        process_logger.log_start()

        tu_table = pyarrow.Table.from_pylist(self.records, schema=self.get_event_schema)
        tu_table = flatten_table_schema(tu_table)
        tu_dataset = process_logger.log_dataframely_filter_results(
            *VehicleTripAssignmentTable.filter(pl.DataFrame(tu_table))
        )

        process_logger.log_complete()
        return tu_dataset


def archive_glides_records(records: list[dict], upload_directory: S3Location, stream_name: str) -> bool:
    """
    Archive raw Glides records to S3 as a gzipped JSON file.

    Args:
        records: List of raw Glides record dictionaries
        upload_directory: S3 location to upload the archive file
        stream_name: Stream name to include in filename

    Returns:
        True if archiving succeeded, False otherwise
    """
    if not records:
        return True

    process_logger = ProcessLogger("archive_glides_records")
    process_logger.log_start()

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_file_name = f"{stream_name}_{timestamp}.json.gz"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json.gz", delete=True) as tmp_file:
            with gzip.open(tmp_file, "wt", encoding="utf-8") as gf:
                dump(records, gf)

            # Flush to ensure data is written before upload
            tmp_file.flush()

            upload_file(
                tmp_file.name,
                os.path.join(upload_directory.s3_uri, archive_file_name),
            )

        process_logger.add_metadata(record_count=len(records), file_name=archive_file_name)
        process_logger.log_complete()
        return True

    except (OSError, IOError) as e:
        process_logger.log_failure(e)
        return False


def ingest_glides_events(
    kinesis_reader: KinesisReader, metadata_queue: Queue[Optional[str]], upload: bool = False
) -> None:
    """Ingest glides records from the kinesis stream and add them to parquet files."""
    process_logger = ProcessLogger(process_name="ingest_glides_events")
    process_logger.log_start()

    try:
        converters = [
            EditorChanges(),
            OperatorSignIns(),
            TripUpdates(),
            VehicleTripAssignment(),
        ]

        records = kinesis_reader.get_records()

        archive_glides_records(records, glides_records, kinesis_reader.stream_name)

        for record in records:
            try:
                # format this so it can be used to partition parquet files
                record["time"] = datetime.fromisoformat(record["time"].replace("Z", "+00:00"))

                data_keys = record["data"].keys()

                for converter in converters:
                    if converter.unique_key in data_keys:
                        converter.records.append(record)
                        break
                else:
                    raise KeyError(f"No distinguishing key in {data_keys}")
            except Exception as e:
                process_logger.log_failure(e)

        for converter in converters:
            converter.append_records()
            if upload:
                converter.upload_records()
            metadata_queue.put(os.path.join(converter.general_directory, converter.base_filename))

    except Exception as e:
        process_logger.log_failure(e)

    process_logger.log_complete()
